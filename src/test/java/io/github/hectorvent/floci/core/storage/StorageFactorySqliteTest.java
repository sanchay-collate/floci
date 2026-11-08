package io.github.hectorvent.floci.core.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class StorageFactorySqliteTest {
    private static final TypeReference<Map<String, String>> TYPE = new TypeReference<>() { };
    @TempDir
    Path directory;

    @Test
    void sqliteSelectionLoadsOnceAndExportsOnShutdown() throws Exception {
        StorageFactory factory = factory("persistent", "sqlite", 1024);
        AccountAwareStorageBackend<String> index = create(factory);
        assertTrue(index.supportsIndex());
        index.put("bucket/key", "value");
        factory.loadAll();
        assertSame(index, create(factory));
        assertEquals(Optional.of("value"), index.get("bucket/key"));
        factory.shutdownAll();
        assertTrue(Files.readString(directory.resolve("s3-objects.json")).contains("value"));
        StorageFactory reopened = factory("persistent", "sqlite", 1024);
        try {
            assertEquals(Optional.of("value"), create(reopened).get("bucket/key"));
        } finally {
            reopened.shutdownAll();
        }
    }

    @Test
    void walModeUsesItsExistingSnapshotName() {
        StorageFactory factory = factory("wal", "sqlite", 1024);
        create(factory).put("bucket/key", "value");
        factory.shutdownAll();
        assertTrue(Files.exists(directory.resolve("s3-objects-snapshot.json")));
        assertFalse(Files.exists(directory.resolve("s3-objects.json")));
    }

    @Test
    void memoryModeDoesNotOpenSqliteOrWriteFiles() throws Exception {
        StorageFactory factory = factory("memory", "sqlite", 1024);
        AccountAwareStorageBackend<String> index = create(factory);
        assertFalse(index.supportsIndex());
        index.put("bucket/key", "value");
        factory.shutdownAll();
        try (Stream<Path> files = Files.list(directory)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void jsonRemainsTheDefaultLayout() {
        StorageFactory factory = factory("memory", "json", 1024);
        assertFalse(create(factory).supportsIndex());
        factory.shutdownAll();
    }

    @Test
    void invalidLayoutAndCacheAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> create(factory("persistent", "typo", 1024)));
        assertThrows(IllegalArgumentException.class, () -> create(factory("persistent", "sqlite", 1)));
    }

    private AccountAwareStorageBackend<String> create(StorageFactory factory) {
        return factory.createIndexed("s3", "s3-objects.json", TYPE, WriteProfile.APPEND_HEAVY,
                (key, value) -> new IndexedStorage.Index("scope", key, "null", Instant.EPOCH, null, 1, false));
    }

    private StorageFactory factory(String mode, String layout, int cache) {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.defaultAccountId()).thenReturn("000000000000");
        when(config.storage().persistentPath()).thenReturn(directory.toString());
        when(config.storage().services().s3().objectIndexLayout()).thenReturn(layout);
        when(config.storage().services().s3().sqliteCacheKib()).thenReturn(cache);
        ServiceConfigAccess access = mock(ServiceConfigAccess.class);
        when(access.storageMode(anyString())).thenReturn(mode);
        lenient().when(access.storageFlushInterval(anyString())).thenReturn(60_000L);
        return new StorageFactory(config, access);
    }
}
