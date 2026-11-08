package io.github.hectorvent.floci.core.storage;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Bounded, ordered access to a document index, without loading a store-wide result list. */
public interface IndexedStorage<V> extends StorageBackend<String, V> {

    enum View { ALL, CURRENT, VERSIONS, HISTORY }

    enum Position { ENTRY, NAME, PREFIX }

    /** Kind 0 is a current version alias, 1 an unversioned current entry, and 2 a history entry. */
    record Index(String scope, String name, String version, Instant modified, String sequence,
                 int kind, boolean deleted) { }

    record Entry<V>(String storageKey, V value, Index index) { }

    record Cursor(Position position, String name, Index index, String storageKey) {
        public static Cursor after(Entry<?> entry) {
            return new Cursor(Position.ENTRY, entry.index().name(), entry.index(), entry.storageKey());
        }

        public static Cursor afterName(String name) {
            return new Cursor(Position.NAME, name, null, null);
        }

        public static Cursor afterPrefix(String prefix) {
            return new Cursor(Position.PREFIX, prefix, null, null);
        }
    }

    @FunctionalInterface
    interface Projection<V> {
        Index project(String storageKey, V value);
    }

    default boolean supportsIndex() {
        return true;
    }

    /** Entries retain raw storage keys, including their account prefix, for cursor stability. */
    List<Entry<V>> page(String scope, String prefix, Cursor after, int limit, View view);

    Optional<Entry<V>> version(String scope, String name, String version);
}
