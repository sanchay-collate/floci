package io.github.hectorvent.floci.core.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.JDBC;
import org.sqlite.ProgressHandler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteStorageTest {
    private static final TypeReference<Map<String, String>> TYPE = new TypeReference<>() { };
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path directory;

    private static SqliteStorage<String> store(Path path) {
        return new SqliteStorage<>(path.resolve("objects.sqlite"), path.resolve("objects.json"),
                path.resolve("objects.wal"), TYPE,
                (key, value) -> new IndexedStorage.Index("scope", key, "null", Instant.EPOCH, null, 1, false), 1024);
    }

    private SqliteStorage<String> open() {
        SqliteStorage<String> storage = store(directory);
        storage.load();
        return storage;
    }

    @Test
    void writesBatchesAndExportsACompatibleSnapshot() throws IOException {
        try (SqliteStorage<String> storage = open()) {
            storage.put("first", "v1");
            storage.putAllAtomically(Map.of("second", "v2", "third", "v3"));
            storage.deleteAllAtomically(Set.of("first", "absent"));
            assertEquals(Set.of("second", "third"), storage.keys());
            assertEquals("v2", storage.get("second").orElseThrow());
        }
        assertEquals(Map.of("second", "v2", "third", "v3"),
                JSON.readValue(directory.resolve("objects.json").toFile(), TYPE));
        assertEquals(0, Files.size(directory.resolve("objects.wal")));
        try (SqliteStorage<String> reloaded = open()) {
            assertEquals("v3", reloaded.get("third").orElseThrow());
        }
    }

    @Test
    void streamsSnapshotAndReplaysEveryLegacyRecordType() throws IOException {
        Path producer = directory.resolve("producer");
        Files.createDirectories(producer);
        WalStorage<String, String> wal = new WalStorage<>(producer.resolve("snapshot.json"),
                producer.resolve("journal.wal"), TYPE, Long.MAX_VALUE);
        wal.load();
        wal.put("first", "updated");
        wal.put("deleted", "remove");
        wal.delete("deleted");
        wal.putAllAtomically(Map.of("batch", "atomic", "batch-deleted", "remove"));
        wal.deleteAllAtomically(Set.of("batch-deleted"));
        Files.copy(producer.resolve("journal.wal"), directory.resolve("objects.wal"));
        wal.shutdown();
        Files.writeString(directory.resolve("objects.json"), "{\"first\":\"old\",\"kept\":\"original\"}");
        byte[] original = Files.readAllBytes(directory.resolve("objects.wal"));
        try (SqliteStorage<String> storage = open()) {
            assertEquals("updated", storage.get("first").orElseThrow());
            assertEquals("original", storage.get("kept").orElseThrow());
            assertEquals("atomic", storage.get("batch").orElseThrow());
            assertFalse(storage.get("deleted").isPresent());
            assertFalse(storage.get("batch-deleted").isPresent());
            assertEquals(HexFormat.of().formatHex(original),
                    HexFormat.of().formatHex(Files.readAllBytes(directory.resolve("objects.wal"))));
        }
        assertEquals(0, Files.size(directory.resolve("objects.wal")));
        assertEquals(original.length, Files.size(directory.resolve("objects.wal.before-sqlite")));
    }

    @Test
    void malformedSnapshotFailsWithoutPublishingAnEmptyStore() throws IOException {
        Path snapshot = directory.resolve("objects.json");
        Files.writeString(snapshot, "{\"first\":\"valid\",\"torn\":");
        assertThrows(IllegalStateException.class, this::open);
        assertEquals("{\"first\":\"valid\",\"torn\":", Files.readString(snapshot));
        Files.writeString(snapshot, "{\"fixed\":\"value\"}");
        try (SqliteStorage<String> storage = open()) {
            assertEquals(Set.of("fixed"), storage.keys());
        }
    }

    @Test
    void atomicBatchRollsBackWhenAnEntryCannotBeEncoded() {
        try (SqliteStorage<String> storage = open()) {
            storage.put("kept", "value");
            Map<String, String> batch = new LinkedHashMap<>();
            batch.put("partial", "must not survive");
            batch.put("invalid", null);
            assertThrows(IllegalStateException.class, () -> storage.putAllAtomically(batch));
            assertEquals(Set.of("kept"), storage.keys());
        }
        try (SqliteStorage<String> storage = open()) {
            assertEquals(Set.of("kept"), storage.keys());
        }
    }

    @Test
    void indexedPagesUseLiteralPrefixesAndStableCursors() {
        try (SqliteStorage<String> storage = open()) {
            storage.putAll(Map.of("a%/one", "v1", "a%/two", "v2", "abc/other", "v3", "z", "v4"));
            List<IndexedStorage.Entry<String>> first = storage.page("scope", "a%/", null, 1, IndexedStorage.View.CURRENT);
            assertEquals("v1", first.getFirst().value());
            assertEquals("a%/one", first.getFirst().index().name());
            List<IndexedStorage.Entry<String>> next = storage.page("scope", "a%/", IndexedStorage.Cursor.after(first.getFirst()),
                    1, IndexedStorage.View.CURRENT);
            assertEquals("a%/two", next.getFirst().index().name());
            assertEquals(List.of("abc/other", "z"), storage.page("scope", "", IndexedStorage.Cursor.afterPrefix("a%/"),
                    5, IndexedStorage.View.CURRENT).stream().map(entry -> entry.index().name()).toList());
        }
    }

    @Test
    void offlineCleanupIsReimportedAfterACleanShutdown() throws IOException {
        try (SqliteStorage<String> storage = open()) {
            storage.putAll(Map.of("expired", "log", "kept", "document"));
        }
        Files.writeString(directory.resolve("objects.json"), "{\"kept\":\"document\"}");
        try (SqliteStorage<String> storage = open()) {
            assertEquals(Set.of("kept"), storage.keys());
        }
    }

    @Test
    void aSecondWriterCannotOpenTheSameIndex() {
        try (SqliteStorage<String> first = open()) {
            assertThrows(IllegalStateException.class, this::open);
            first.put("still", "usable");
            assertEquals("usable", first.get("still").orElseThrow());
        }
    }

    @Test
    void committedWritesSurviveAnAbruptProcessExit() throws Exception {
        prepareCrash();
        try (SqliteStorage<String> storage = open()) {
            assertEquals("durable", storage.get("crash").orElseThrow());
        }
    }

    @Test
    void conflictingLegacyEditsAfterACrashAreRefused() throws Exception {
        prepareCrash();
        Files.writeString(directory.resolve("objects.json"), "{\"external\":\"edit\"}");
        IllegalStateException failure = assertThrows(IllegalStateException.class, this::open);
        assertTrue(failure.getCause().getMessage().contains("both changed"));
        assertEquals("{\"external\":\"edit\"}", Files.readString(directory.resolve("objects.json")));
    }

    @Test
    void unfinishedExportsRecoverBeforeAndAfterSnapshotRename() throws Exception {
        for (boolean renamed : List.of(false, true)) {
            Path path = directory.resolve(Boolean.toString(renamed));
            Files.createDirectories(path);
            prepareCrash(path);
            Path snapshot = path.resolve("objects.json");
            Path wal = path.resolve("objects.wal");
            String exported = JSON.writeValueAsString(Map.of("original", "value", "crash", "durable"));
            Map<String, String> pending = Map.of("export.json", sha(exported.getBytes(StandardCharsets.UTF_8)),
                    "export.old_json", sha(Files.readAllBytes(snapshot)),
                    "export.old_wal", sha(Files.readAllBytes(wal)), "export.revision", "2");
            try (Connection connection = JDBC.createConnection("jdbc:sqlite:" + path.resolve("objects.sqlite"), new Properties());
                 PreparedStatement insert = connection.prepareStatement("INSERT INTO meta VALUES(?,?)")) {
                for (Map.Entry<String, String> entry : pending.entrySet()) {
                    insert.setString(1, entry.getKey());
                    insert.setString(2, entry.getValue());
                    insert.executeUpdate();
                }
            }
            Files.writeString(renamed ? snapshot : path.resolve("objects.json.sqlite-export.tmp"), exported);
            try (SqliteStorage<String> recovered = store(path)) {
                recovered.load();
                assertEquals("durable", recovered.get("crash").orElseThrow());
                assertEquals(Map.of("original", "value", "crash", "durable"), JSON.readValue(snapshot.toFile(), TYPE));
            }
        }
    }

    @Test
    void tornWalTailIsPreservedAndOnlyCompleteRecordsAreImported() throws Exception {
        try (SqliteStorage<String> storage = open()) {
            storage.put("kept", "value");
        }
        Path wal = directory.resolve("objects.wal");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(wal))) {
            output.writeByte(WalStorage.OP_BATCH);
            output.writeInt(200);
            output.write(new byte[] {1, 2, 3});
        }
        byte[] original = Files.readAllBytes(wal);
        try (SqliteStorage<String> storage = open()) {
            assertEquals(Set.of("kept"), storage.keys());
            assertEquals(original.length, Files.size(wal));
        }
        assertEquals(0, Files.size(wal));
    }

    @Test
    void oversizedLegacyFramesRequireOldVersionCompaction() throws Exception {
        Path wal = directory.resolve("objects.wal");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(wal))) {
            output.writeByte(WalStorage.OP_BATCH);
            output.writeInt(16 * 1024 * 1024 + 1);
            output.write(new byte[16 * 1024 * 1024 + 1]);
        }
        assertThrows(IllegalStateException.class, this::open);
        assertEquals(16 * 1024 * 1024 + 6, Files.size(wal));
    }

    @Test
    void writesUpdateTheCleanupActivityMarkerWithoutLegacyWalGrowth() throws Exception {
        try (SqliteStorage<String> storage = open()) {
            Path wal = directory.resolve("objects.wal");
            FileTime old = FileTime.from(Instant.parse("2020-01-01T00:00:00Z"));
            Files.setLastModifiedTime(wal, old);
            storage.put("activity", "value");
            assertTrue(Files.getLastModifiedTime(wal).compareTo(old) > 0);
            assertEquals(0, Files.size(wal));
        }
    }

    @Test
    void externalEditsWhileRunningAreNeverOverwrittenByExport() throws Exception {
        SqliteStorage<String> storage = open();
        storage.put("kept", "value");
        storage.flush();
        Path snapshot = directory.resolve("objects.json");
        String original = Files.readString(snapshot);
        Files.writeString(snapshot, "{\"external\":\"edit\"}");
        assertThrows(IllegalStateException.class, storage::flush);
        assertEquals("{\"external\":\"edit\"}", Files.readString(snapshot));
        Files.writeString(snapshot, original);
        storage.close();
    }

    @Test
    void removingAnImportedNonemptyWalCannotDiscardItsRecords() throws Exception {
        Path producer = directory.resolve("producer");
        Files.createDirectories(producer);
        WalStorage<String, String> wal = new WalStorage<>(producer.resolve("snapshot.json"),
                producer.resolve("journal.wal"), TYPE, Long.MAX_VALUE);
        wal.load();
        wal.put("durable", "from journal");
        Files.copy(producer.resolve("journal.wal"), directory.resolve("objects.wal"));
        wal.shutdown();
        Files.writeString(directory.resolve("objects.json"), "{}");
        SqliteStorage<String> storage = open();
        assertEquals("from journal", storage.get("durable").orElseThrow());
        Files.delete(directory.resolve("objects.wal"));
        assertThrows(IllegalStateException.class, storage::close);
        assertThrows(IllegalStateException.class, this::open);
        assertFalse(Files.exists(directory.resolve("objects.wal")));
        assertThrows(IllegalStateException.class, this::open);
        assertTrue(Files.size(directory.resolve("objects.wal.before-sqlite")) > 0);
    }

    @Test
    void anErrorCannotCommitAPartiallyEncodedBatch() {
        IndexedStorage.Projection<String> projection = (key, value) -> {
            if ("fatal".equals(value)) {
                throw new AssertionError("encoding aborted");
            }
            return new IndexedStorage.Index("scope", key, "null", Instant.EPOCH, null, 1, false);
        };
        try (SqliteStorage<String> storage = new SqliteStorage<>(directory.resolve("objects.sqlite"),
                directory.resolve("objects.json"), directory.resolve("objects.wal"), TYPE, projection, 1024)) {
            storage.load();
            storage.put("kept", "value");
            Map<String, String> batch = new LinkedHashMap<>();
            batch.put("partial", "must roll back");
            batch.put("aborted", "fatal");
            assertThrows(AssertionError.class, () -> storage.putAllAtomically(batch));
            assertEquals(Set.of("kept"), storage.keys());
        }
        try (SqliteStorage<String> storage = open()) {
            assertEquals(Set.of("kept"), storage.keys());
        }
    }

    @Test
    void cursorsSeekPastLargeRangesWithoutScanningTheSkippedRows() throws Exception {
        try (SqliteStorage<String> storage = open()) {
            Map<String, String> batch = new LinkedHashMap<>();
            for (int i = 0; i < 50_000; i++) {
                batch.put("group/key-" + String.format("%05d", i), "value");
            }
            storage.putAll(batch);
            IndexedStorage.Entry<String> late = storage.page("scope", "group/key-49990", null, 1,
                    IndexedStorage.View.CURRENT).getFirst();
            Field field = SqliteStorage.class.getDeclaredField("connection");
            field.setAccessible(true);
            Connection connection = (Connection) field.get(storage);
            ProgressHandler.setHandler(connection, 10_000, new ProgressHandler() {
                @Override
                protected int progress() {
                    return 1;
                }
            });
            try {
                assertTrue(storage.page("scope", "", IndexedStorage.Cursor.afterPrefix("group/"), 1,
                        IndexedStorage.View.CURRENT).isEmpty());
                assertEquals("group/key-49991", storage.page("scope", "group/", IndexedStorage.Cursor.after(late),
                        1, IndexedStorage.View.CURRENT).getFirst().index().name());
                assertEquals("group/key-49991", storage.page("scope", "group/", IndexedStorage.Cursor.afterName("group/key-49990"),
                        1, IndexedStorage.View.CURRENT).getFirst().index().name());
                assertTrue(storage.page("scope", "group/key-4", IndexedStorage.Cursor.afterPrefix("group/"),
                        1, IndexedStorage.View.CURRENT).isEmpty());
            } finally {
                ProgressHandler.clearHandler(connection);
            }
        }
    }

    private void prepareCrash() throws Exception {
        prepareCrash(directory);
    }

    private static void prepareCrash(Path path) throws Exception {
        try (SqliteStorage<String> storage = store(path)) {
            storage.load();
            storage.put("original", "value");
        }
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED", "-Xmx96m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                CrashWriter.class.getName(), path.toString()).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "crash writer must finish");
            assertEquals(0, process.exitValue(), new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    public static final class CrashWriter {
        public static void main(String[] args) {
            SqliteStorage<String> storage = store(Path.of(args[0]));
            storage.load();
            storage.put("crash", "durable");
            Runtime.getRuntime().halt(0);
        }
    }
}
