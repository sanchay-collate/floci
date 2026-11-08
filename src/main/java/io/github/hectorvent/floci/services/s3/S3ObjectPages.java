package io.github.hectorvent.floci.services.s3;

import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.IndexedStorage;
import io.github.hectorvent.floci.services.s3.model.S3Object;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

/** S3 pagination over bounded database pages, including delimiter seeks and version markers. */
final class S3ObjectPages {
    private static final int PAGE_SIZE = 128;
    private final IndexedStorage<S3Object> index;

    S3ObjectPages(IndexedStorage<S3Object> index) {
        this.index = index;
    }

    S3Service.ListObjectsResult listObjects(String account, String bucket, String prefix, String delimiter,
            int maxKeys, String continuation, String startAfter) {
        String marker = continuation != null ? continuation : startAfter;
        Page result = read(account, bucket, prefix, delimiter, maxKeys, marker,
                marker == null ? null : IndexedStorage.Cursor.afterName(marker), IndexedStorage.View.CURRENT);
        return new S3Service.ListObjectsResult(result.objects(), result.prefixes(), result.truncated(),
                result.truncated() ? result.lastKey() : null);
    }

    S3Service.ListVersionsResult listVersions(String account, String bucket, String prefix, String delimiter,
            int maxKeys, String keyMarker, String versionMarker) {
        IndexedStorage.Cursor cursor = null;
        if (keyMarker != null && !keyMarker.isEmpty()) {
            cursor = versionMarker == null || versionMarker.isEmpty()
                    ? IndexedStorage.Cursor.afterName(keyMarker)
                    : version(account, bucket, keyMarker, versionMarker).map(IndexedStorage.Cursor::after)
                            .orElseGet(() -> IndexedStorage.Cursor.afterName(keyMarker));
        }
        Page result = read(account, bucket, prefix, delimiter, maxKeys, keyMarker, cursor,
                IndexedStorage.View.VERSIONS);
        return new S3Service.ListVersionsResult(result.objects(), result.prefixes(), result.truncated(),
                result.truncated() ? result.lastKey() : null,
                result.truncated() ? result.lastVersion() : null);
    }

    boolean hasEntries(String account, String bucket) {
        return !page(account, bucket, "", null, 1, IndexedStorage.View.ALL).isEmpty();
    }

    Optional<S3Object> newestHistory(String account, String bucket, String key) {
        List<IndexedStorage.Entry<S3Object>> page = page(account, bucket, key, null, 1, IndexedStorage.View.HISTORY);
        return page.stream().filter(entry -> entry.index().name().equals(key)).map(IndexedStorage.Entry::value).findFirst();
    }

    record History(S3Object current, List<S3Object> versions) { }

    /** Visits one logical key at a time, retaining only that key's version history and one page. */
    boolean histories(String account, String bucket, BiPredicate<String, History> visitor) {
        IndexedStorage.Cursor cursor = null;
        String key = null;
        S3Object current = null;
        List<S3Object> versions = new ArrayList<>();
        while (true) {
            List<IndexedStorage.Entry<S3Object>> batch = page(account, bucket, "", cursor, PAGE_SIZE,
                    IndexedStorage.View.ALL);
            for (IndexedStorage.Entry<S3Object> entry : batch) {
                if (key != null && !key.equals(entry.index().name())) {
                    if (!visitor.test(key, new History(current, versions))) {
                        return false;
                    }
                    current = null;
                    versions = new ArrayList<>();
                }
                key = entry.index().name();
                if (entry.index().kind() == 2) {
                    versions.add(entry.value());
                } else {
                    current = entry.value();
                }
                cursor = IndexedStorage.Cursor.after(entry);
            }
            if (batch.size() < PAGE_SIZE) {
                return key == null || visitor.test(key, new History(current, versions));
            }
        }
    }

    private record Page(List<S3Object> objects, List<String> prefixes, boolean truncated,
                        String lastKey, String lastVersion) { }

    private Page read(String account, String bucket, String prefix, String delimiter, int maxKeys,
                      String marker, IndexedStorage.Cursor initial, IndexedStorage.View view) {
        List<S3Object> objects = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        if (maxKeys == 0) {
            return new Page(objects, prefixes, false, null, null);
        }
        String effectivePrefix = prefix == null ? "" : prefix;
        int limit = maxKeys < 0 ? Integer.MAX_VALUE : maxKeys;
        String lastKey = null;
        String lastVersion = null;
        IndexedStorage.Cursor cursor = initial;
        while (true) {
            List<IndexedStorage.Entry<S3Object>> batch = page(account, bucket, effectivePrefix, cursor,
                    PAGE_SIZE, view);
            boolean seek = false;
            for (IndexedStorage.Entry<S3Object> entry : batch) {
                String key = entry.index().name();
                int delimiterIndex = delimiter == null || delimiter.isEmpty() ? -1
                        : key.indexOf(delimiter, effectivePrefix.length());
                String group = delimiterIndex < 0 ? null : key.substring(0, delimiterIndex + delimiter.length());
                cursor = IndexedStorage.Cursor.after(entry);
                if (group != null && marker != null && compare(group, marker) <= 0) {
                    cursor = IndexedStorage.Cursor.afterPrefix(group);
                    seek = true;
                    break;
                }
                if (objects.size() + prefixes.size() == limit) {
                    return new Page(objects, prefixes, true, lastKey, lastVersion);
                }
                if (group == null) {
                    objects.add(entry.value());
                    lastKey = key;
                    lastVersion = view == IndexedStorage.View.VERSIONS ? entry.index().version() : null;
                } else {
                    prefixes.add(group);
                    lastKey = group;
                    lastVersion = null;
                    cursor = IndexedStorage.Cursor.afterPrefix(group);
                    seek = true;
                    break;
                }
            }
            if (!seek && batch.size() < PAGE_SIZE) {
                return new Page(objects, prefixes, false, lastKey, lastVersion);
            }
        }
    }

    private List<IndexedStorage.Entry<S3Object>> page(String account, String bucket, String prefix,
            IndexedStorage.Cursor after, int limit, IndexedStorage.View view) {
        if (index instanceof AccountAwareStorageBackend<S3Object> aware) {
            return account == null ? aware.page(bucket, prefix, after, limit, view)
                    : aware.pageForAccount(account, bucket, prefix, after, limit, view);
        }
        return index.page(account + "/" + bucket, prefix, after, limit, view);
    }

    private Optional<IndexedStorage.Entry<S3Object>> version(String account, String bucket, String name, String version) {
        if (index instanceof AccountAwareStorageBackend<S3Object> aware) {
            return account == null ? aware.version(bucket, name, version)
                    : aware.versionForAccount(account, bucket, name, version);
        }
        return index.version(account + "/" + bucket, name, version);
    }

    private static int compare(String left, String right) {
        return Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }
}
