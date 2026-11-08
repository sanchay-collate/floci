package io.github.hectorvent.floci.services.s3;

import io.github.hectorvent.floci.core.storage.IndexedStorage;
import io.github.hectorvent.floci.services.s3.model.S3Object;

import java.time.Instant;
import java.util.Objects;

/** Indexes logical object names independently of the legacy version-key encoding. */
final class S3ObjectProjection implements IndexedStorage.Projection<S3Object> {
    private final String defaultAccount;

    S3ObjectProjection(String defaultAccount) {
        this.defaultAccount = defaultAccount;
    }

    @Override
    public IndexedStorage.Index project(String storageKey, S3Object object) {
        boolean scoped = storageKey.length() > 13 && storageKey.charAt(12) == '/'
                && storageKey.substring(0, 12).chars().allMatch(character -> character >= '0' && character <= '9');
        String account = scoped ? storageKey.substring(0, 12) : defaultAccount;
        String relativeKey = scoped ? storageKey.substring(13) : storageKey;
        String bucket = Objects.requireNonNull(object.getBucketName(), "S3 metadata has no bucket");
        String key = Objects.requireNonNull(object.getKey(), "S3 metadata has no object key");
        boolean current = relativeKey.equals(bucket + "/" + key);
        String version = object.getVersionId() == null ? "null" : object.getVersionId();
        if (!current && !relativeKey.equals(S3Service.versionedKey(bucket, key, version))
                && !relativeKey.equals(bucket + "/" + key + "#v#" + version)) {
            throw new IllegalArgumentException("S3 metadata fields disagree with storage key: " + storageKey);
        }
        int kind = current ? (object.getVersionId() == null ? 1 : 0) : 2;
        return new IndexedStorage.Index(account + "/" + bucket, key,
                version,
                object.getLastModified() == null ? Instant.EPOCH : object.getLastModified(),
                object.getEventSequencer(), kind, object.isDeleteMarker());
    }
}
