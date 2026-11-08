package io.github.hectorvent.floci.core.storage;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.jboss.logging.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Streams a legacy JSON snapshot and replays its journal without modifying either source. */
final class LegacyStoreReader<V> {
    private static final Logger LOG = Logger.getLogger(LegacyStoreReader.class);
    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    private final ObjectMapper json;
    private final ObjectMapper cbor = new ObjectMapper(new CBORFactory()).registerModule(new JavaTimeModule());
    private final JavaType valueType;
    private final JavaType mapType;

    LegacyStoreReader(ObjectMapper json, TypeReference<Map<String, V>> type) {
        this.json = json;
        this.mapType = json.constructType(type.getType());
        this.valueType = mapType.getContentType();
    }

    void read(Path snapshot, Path wal, BiConsumer<String, V> put, Consumer<String> delete) throws IOException {
        if (Files.exists(snapshot)) {
            try (JsonParser parser = json.createParser(snapshot.toFile())) {
                if (parser.nextToken() != JsonToken.START_OBJECT) {
                    throw new IOException("Legacy snapshot is not a JSON object: " + snapshot);
                }
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    if (parser.currentToken() != JsonToken.FIELD_NAME) {
                        throw new IOException("Incomplete legacy snapshot: " + snapshot);
                    }
                    String key = parser.currentName();
                    parser.nextToken();
                    V value = json.readValue(parser, valueType);
                    if (value == null) {
                        throw new IOException("Null metadata at " + key + " in " + snapshot);
                    }
                    put.accept(key, value);
                }
                if (parser.nextToken() != null) {
                    throw new IOException("Trailing content in legacy snapshot: " + snapshot);
                }
            }
        }
        if (Files.exists(wal)) {
            readWal(wal, put, delete);
        }
    }

    private void readWal(Path wal, BiConsumer<String, V> put, Consumer<String> delete) throws IOException {
        long size = Files.size(wal);
        long consumed = 0;
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(wal)))) {
            while (consumed < size) {
                try {
                    int operation = input.readUnsignedByte();
                    byte[] first = frame(input, size - consumed - 1);
                    long length = 1L + Integer.BYTES + first.length;
                    switch (operation) {
                        case WalStorage.OP_PUT -> {
                            byte[] payload = frame(input, size - consumed - length);
                            length += Integer.BYTES + payload.length;
                            String key = cbor.readValue(first, String.class);
                            V value = cbor.readValue(payload, valueType);
                            if (key == null || value == null) {
                                throw new EOFException("Null PUT record");
                            }
                            put.accept(key, value);
                        }
                        case WalStorage.OP_DELETE -> {
                            String key = cbor.readValue(first, String.class);
                            if (key == null) {
                                throw new EOFException("Null DELETE record");
                            }
                            delete.accept(key);
                        }
                        case WalStorage.OP_BATCH -> {
                            Map<String, V> batch = cbor.readValue(first, mapType);
                            if (batch == null || batch.entrySet().stream()
                                    .anyMatch(entry -> entry.getKey() == null || entry.getValue() == null)) {
                                throw new EOFException("Invalid atomic PUT batch");
                            }
                            batch.forEach(put);
                        }
                        case WalStorage.OP_DELETE_BATCH -> {
                            List<String> keys = cbor.readValue(first,
                                    cbor.getTypeFactory().constructCollectionType(List.class, String.class));
                            if (keys == null || keys.stream().anyMatch(key -> key == null)) {
                                throw new EOFException("Invalid atomic DELETE batch");
                            }
                            keys.forEach(delete);
                        }
                        default -> throw new EOFException("Unknown operation " + operation);
                    }
                    consumed += length;
                } catch (EOFException | JsonProcessingException exception) {
                    LOG.warnv("Ignoring unreadable legacy WAL tail at offset {0} in {1}: {2}",
                            consumed, wal, exception.getMessage());
                    return;
                }
            }
        }
    }

    private static byte[] frame(DataInputStream input, long remaining) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > remaining - Integer.BYTES) {
            throw new EOFException("Incomplete WAL frame");
        }
        if (length > MAX_FRAME_BYTES) {
            throw new IOException("Legacy WAL frame exceeds 16 MiB; compact the journal with the old "
                    + "Floci version before importing it into SQLite");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("Incomplete WAL payload");
        }
        return bytes;
    }
}
