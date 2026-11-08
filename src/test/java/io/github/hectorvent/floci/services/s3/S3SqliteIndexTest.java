package io.github.hectorvent.floci.services.s3;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.core.storage.IndexedStorage;
import io.github.hectorvent.floci.core.storage.SqliteStorage;
import io.github.hectorvent.floci.services.s3.model.Bucket;
import io.github.hectorvent.floci.services.s3.model.S3Object;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SqliteIndexTest {
    private static final String ACCOUNT = "000000000000";
    private static final String BUCKET = "sqlite-bucket";
    private static final TypeReference<Map<String, S3Object>> TYPE = new TypeReference<>() { };

    @TempDir
    Path directory;
    private final InMemoryStorage<String, Bucket> buckets = new InMemoryStorage<>();
    private SqliteStorage<S3Object> sqlite;
    private AccountAwareStorageBackend<S3Object> objects;
    private BoundedOnly bounded;
    private S3Service service;

    @BeforeEach
    void open() {
        sqlite = new SqliteStorage<>(directory.resolve("objects.sqlite"), directory.resolve("objects.json"),
                directory.resolve("objects.wal"), TYPE, new S3ObjectProjection(ACCOUNT), 1024);
        sqlite.load();
        bounded = new BoundedOnly(sqlite);
        objects = new AccountAwareStorageBackend<>(bounded, null, ACCOUNT);
        service = new S3Service(buckets, objects, directory.resolve("s3"), false);
        if (buckets.get(BUCKET).isEmpty()) {
            service.createBucket(BUCKET, "us-east-1");
        }
    }

    @AfterEach
    void close() {
        sqlite.close();
    }

    @Test
    void objectPaginationAndDelimiterSeeksNeverScanTheWholeStore() {
        Map<String, S3Object> entries = new LinkedHashMap<>();
        for (int i = 0; i < 2000; i++) {
            String key = "group/object-" + String.format("%05d", i);
            entries.put(BUCKET + "/" + key, object(key));
        }
        entries.put(BUCKET + "/tail", object("tail"));
        objects.putAll(entries);
        S3Service.ListObjectsResult first = service.listObjectsWithPrefixes(BUCKET, null, null, 3);
        assertEquals(3, first.objects().size());
        assertTrue(first.isTruncated());
        S3Service.ListObjectsResult next = service.listObjectsWithPrefixes(BUCKET, null, null, 3,
                first.nextContinuationToken(), null);
        assertEquals("group/object-00003", next.objects().getFirst().getKey());
        int before = bounded.pages;
        S3Service.ListObjectsResult groups = service.listObjectsWithPrefixes(BUCKET, null, "/", 1);
        assertEquals(List.of("group/"), groups.commonPrefixes());
        assertTrue(groups.isTruncated());
        assertTrue(bounded.pages - before <= 2, "a delimiter group must be skipped with an indexed seek");
        S3Service.ListObjectsResult tail = service.listObjectsWithPrefixes(BUCKET, null, "/", 1,
                groups.nextContinuationToken(), null);
        assertEquals("tail", tail.objects().getFirst().getKey());
        assertFalse(tail.isTruncated());
        assertTrue(service.listObjectsWithPrefixes(BUCKET, null, null, 0).objects().isEmpty());
        assertEquals("BucketNotEmpty", assertThrows(AwsException.class, () -> service.deleteBucket(BUCKET)).getErrorCode());
    }

    @Test
    void versionsResumeInsideAKeyAndDeleteMarkersSurviveRestart() {
        service.putBucketVersioning(BUCKET, "Enabled");
        S3Object first = put("key", "first");
        S3Object second = put("key", "second");
        S3Object marker = service.deleteObject(BUCKET, "key", null);
        S3Service.ListVersionsResult page = service.listObjectVersions(BUCKET, null, null, 1, null, null);
        assertTrue(page.isTruncated());
        assertTrue(page.versions().getFirst().isDeleteMarker());
        assertEquals(marker.getVersionId(), page.nextVersionIdMarker());
        S3Service.ListVersionsResult rest = service.listObjectVersions(BUCKET, null, null, 10,
                page.nextKeyMarker(), page.nextVersionIdMarker());
        assertEquals(List.of(second.getVersionId(), first.getVersionId()),
                rest.versions().stream().map(S3Object::getVersionId).toList());
        sqlite.close();
        open();
        assertTrue(service.listObjectVersions(BUCKET, null, null, 10, null, null).versions().getFirst().isDeleteMarker());
        assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), service.getObject(BUCKET, "key", first.getVersionId()).getData());
    }

    @Test
    void nullVersionPromotionPreservesBodyTagsAndGeneration() throws Exception {
        put("key", "unversioned");
        service.putBucketVersioning(BUCKET, "Enabled");
        S3Object numbered = put("key", "numbered");
        service.putObjectTagging(BUCKET, "key", "null", Map.of("keep", "tag"));
        service.deleteObject(BUCKET, "key", numbered.getVersionId());
        S3Object current = service.getObject(BUCKET, "key");
        assertArrayEquals("unversioned".getBytes(StandardCharsets.UTF_8), current.getData());
        assertNotNull(current.getDataGeneration());
        assertEquals(Map.of("keep", "tag"), service.getObjectTagging(BUCKET, "key"));
        assertEquals(1, service.listObjectVersions(BUCKET, null, null, 10, null, null).versions().size());
        String generation = current.getDataGeneration();
        sqlite.close();
        assertFalse(Files.readString(directory.resolve("objects.json")).contains("dW52ZXJzaW9uZWQ="));
        open();
        assertEquals(generation, service.getObjectMetadata(BUCKET, "key", null).getDataGeneration());
        assertArrayEquals("unversioned".getBytes(StandardCharsets.UTF_8), service.getObject(BUCKET, "key").getData());
    }

    @Test
    void lifecycleVisitsOneKeyHistoryAtATime() {
        for (int i = 0; i < 5; i++) {
            put("logs/expired-" + i, "log");
        }
        put("documents/kept", "document");
        service.putBucketLifecycle(BUCKET, "<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status>"
                + "<Filter><Prefix>logs/</Prefix></Filter><Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>"
                + "</Rule></LifecycleConfiguration>", null);
        service.applyLifecycleExpiration(Instant.now());
        assertEquals(List.of("documents/kept"), service.listObjects(BUCKET, null, null, 100).stream()
                .map(S3Object::getKey).toList());
    }

    @Test
    void logicalObjectNamesAndAccountsStaySeparateFromVersionKeys() {
        S3Object normal = object("literal#v#key");
        sqlite.put(ACCOUNT + "/" + BUCKET + "/literal#v#key", normal);
        S3Object other = object("foreign");
        sqlite.put("000000000002/" + BUCKET + "/foreign", other);
        assertEquals(List.of("literal#v#key"), service.listObjects(BUCKET, null, null, 100).stream()
                .map(S3Object::getKey).toList());
        S3ObjectPages pages = new S3ObjectPages(sqlite);
        assertEquals(List.of("foreign"), pages.listObjects("000000000002", BUCKET, null, null, 100, null, null)
                .objects().stream().map(S3Object::getKey).toList());
    }

    @Test
    void unicodeKeysUseUtf8OrderAndLegacyAliasesAreNotDuplicated() {
        Map<String, S3Object> rows = new LinkedHashMap<>();
        rows.put(BUCKET + "/legacy", object("legacy"));
        rows.put(ACCOUNT + "/" + BUCKET + "/legacy", object("legacy"));
        rows.put(ACCOUNT + "/" + BUCKET + "/\ue000", object("\ue000"));
        rows.put(ACCOUNT + "/" + BUCKET + "/\ud83d\ude00", object("\ud83d\ude00"));
        sqlite.putAll(rows);
        assertEquals(List.of("legacy", "\ue000", "\ud83d\ude00"), service.listObjects(BUCKET, null, null, 100).stream()
                .map(S3Object::getKey).toList());
    }

    @Test
    void sameTimestampVersionsUseSequencersAcrossPaginationBoundaries() {
        Map<String, S3Object> rows = new LinkedHashMap<>();
        for (int i = 1; i <= 3; i++) {
            S3Object version = object("tie");
            version.setVersionId("version-" + i);
            version.setLastModified(Instant.parse("2026-01-01T00:00:00Z"));
            version.setEventSequencer(String.format("%016X", i));
            rows.put(ACCOUNT + "/" + BUCKET + "/tie#v#version-" + i, version);
            if (i == 3) {
                rows.put(ACCOUNT + "/" + BUCKET + "/tie", version);
            }
        }
        sqlite.putAll(rows);
        S3Service.ListVersionsResult first = service.listObjectVersions(BUCKET, "tie", null, 1, null, null);
        assertEquals("version-3", first.versions().getFirst().getVersionId());
        S3Service.ListVersionsResult second = service.listObjectVersions(BUCKET, "tie", null, 10,
                first.nextKeyMarker(), first.nextVersionIdMarker());
        assertEquals(List.of("version-2", "version-1"), second.versions().stream().map(S3Object::getVersionId).toList());
    }

    @Test
    void deletingACanonicalEntryCannotResurrectItsLegacyAlias() {
        sqlite.put(BUCKET + "/legacy", object("legacy"));
        sqlite.put(ACCOUNT + "/" + BUCKET + "/legacy", object("legacy"));
        service.deleteObject(BUCKET, "legacy");
        assertTrue(service.listObjects(BUCKET, null, null, 100).isEmpty());
        sqlite.put(BUCKET + "/legacy-batch", object("legacy-batch"));
        sqlite.put(ACCOUNT + "/" + BUCKET + "/legacy-batch", object("legacy-batch"));
        objects.deleteAllAtomically(Set.of(BUCKET + "/legacy-batch"));
        assertTrue(service.listObjects(BUCKET, null, null, 100).isEmpty());
        sqlite.put(BUCKET + "/owned", object("owned"));
        objects.deleteForAccount("000000000002", BUCKET + "/owned");
        assertEquals(List.of("owned"), service.listObjects(BUCKET, null, null, 100).stream().map(S3Object::getKey).toList());
    }

    private static S3Object object(String key) {
        return new S3Object(BUCKET, key, 4L, "text/plain", "\"etag\"");
    }

    private S3Object put(String key, String body) {
        return service.putObject(BUCKET, key, body.getBytes(StandardCharsets.UTF_8), "text/plain", Map.of());
    }

    /** Any accidental fallback to the old full-store scans makes the test fail immediately. */
    private static final class BoundedOnly implements IndexedStorage<S3Object> {
        private final SqliteStorage<S3Object> delegate;
        private int pages;

        private BoundedOnly(SqliteStorage<S3Object> delegate) { this.delegate = delegate; }
        @Override public void put(String key, S3Object value) { delegate.put(key, value); }
        @Override public void putAll(Map<String, S3Object> values) { delegate.putAll(values); }
        @Override public Optional<S3Object> get(String key) { return delegate.get(key); }
        @Override public void delete(String key) { delegate.delete(key); }
        @Override public List<S3Object> scan(Predicate<String> filter) { throw new AssertionError("unbounded scan"); }
        @Override public Set<String> keys() { throw new AssertionError("unbounded key scan"); }
        @Override public void flush() { delegate.flush(); }
        @Override public void load() { delegate.load(); }
        @Override public void clear() { delegate.clear(); }
        @Override public List<Entry<S3Object>> page(String scope, String prefix, Cursor after, int limit, View view) {
            pages++;
            assertTrue(limit <= 128);
            return delegate.page(scope, prefix, after, limit, view);
        }
        @Override public void deleteAllAtomically(Set<String> keys) { delegate.deleteAllAtomically(keys); }
        @Override public Optional<Entry<S3Object>> version(String scope, String name, String version) {
            return delegate.version(scope, name, version);
        }
    }
}
