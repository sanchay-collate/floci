package io.github.hectorvent.floci.core.storage;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.jboss.logging.Logger;
import org.sqlite.JDBC;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/** A transactional document index with a bounded SQLite page cache and no in-memory object map. */
public final class SqliteStorage<V> implements IndexedStorage<V>, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(SqliteStorage.class);
    private static final int KEY_PAGE = 256;
    private static final String EMPTY_HASH = hash(new byte[0]);
    private static final Set<Path> OPEN_DATABASES = ConcurrentHashMap.newKeySet();
    private static final String UPSERT = "INSERT INTO entries VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            + "ON CONFLICT(k) DO UPDATE SET payload=excluded.payload,scope=excluded.scope,"
            + "name=excluded.name,version=excluded.version,t=excluded.t,n=excluded.n,"
            + "s=excluded.s,r=excluded.r,kind=excluded.kind,deleted=excluded.deleted";

    private final Path database;
    private final Path snapshot;
    private final Path legacyWal;
    private final Path exportFile;
    private final TypeReference<Map<String, V>> type;
    private final Projection<V> projection;
    private final int cacheKiB;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final JavaType valueType;
    private Connection connection;
    private FileChannel leaseChannel;
    private FileLock lease;
    private PreparedStatement upsert;
    private PreparedStatement remove;
    private boolean failed;
    private Path processLease;

    public SqliteStorage(Path database, Path snapshot, Path legacyWal,
                         TypeReference<Map<String, V>> type, Projection<V> projection, int cacheKiB) {
        this.database = database;
        this.snapshot = snapshot;
        this.legacyWal = legacyWal;
        this.exportFile = snapshot.resolveSibling(snapshot.getFileName() + ".sqlite-export.tmp");
        this.type = type;
        this.projection = projection;
        this.cacheKiB = cacheKiB;
        this.valueType = mapper.constructType(type.getType()).getContentType();
        if (cacheKiB < 64 || cacheKiB > 1_048_576) {
            throw new IllegalArgumentException("SQLite cache must be between 64 and 1048576 KiB");
        }
    }

    @Override
    public synchronized void load() {
        if (connection != null) {
            return;
        }
        failed = false;
        try {
            Files.createDirectories(database.toAbsolutePath().getParent());
            Path physical = database.toAbsolutePath().getParent().toRealPath().resolve(database.getFileName());
            if (!OPEN_DATABASES.add(physical)) {
                throw new IllegalStateException("SQLite metadata is already open in this process: " + database);
            }
            processLease = physical;
            leaseChannel = FileChannel.open(database.resolveSibling(database.getFileName() + ".lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            lease = leaseChannel.tryLock();
            if (lease == null) {
                throw new IllegalStateException("SQLite metadata is already in use: " + database);
            }
            boolean missingLegacyWal = !Files.exists(legacyWal);
            connection = JDBC.createConnection("jdbc:sqlite:" + database.toAbsolutePath(), new Properties());
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA busy_timeout=5000");
                statement.execute("PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA synchronous=FULL");
                statement.execute("PRAGMA cache_size=-" + cacheKiB);
                statement.execute("PRAGMA mmap_size=0");
                statement.execute("PRAGMA temp_store=FILE");
                statement.execute("PRAGMA wal_autocheckpoint=1000");
                statement.execute("PRAGMA journal_size_limit=67108864");
                try (ResultSet version = statement.executeQuery("PRAGMA user_version")) {
                    if (version.next() && version.getInt(1) > 1) {
                        throw new IllegalStateException("Unsupported SQLite metadata schema: " + database);
                    }
                }
                statement.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL) WITHOUT ROWID");
                // A rowid table avoids overflow pages for kilobyte-sized metadata documents.
                statement.execute("CREATE TABLE IF NOT EXISTS entries(k BLOB PRIMARY KEY NOT NULL,payload BLOB NOT NULL,"
                        + "scope TEXT NOT NULL,name BLOB NOT NULL,version TEXT NOT NULL,t INTEGER NOT NULL,"
                        + "n INTEGER NOT NULL,s BLOB NOT NULL,r BLOB NOT NULL,kind INTEGER NOT NULL,"
                        + "deleted INTEGER NOT NULL)");
                statement.execute("CREATE INDEX IF NOT EXISTS entries_order ON entries(scope,name,t,n,s,r,k)");
                statement.execute("CREATE INDEX IF NOT EXISTS entries_version ON entries(scope,name,version,kind,k)");
                statement.execute("PRAGMA user_version=1");
            }
            upsert = connection.prepareStatement(UPSERT);
            remove = connection.prepareStatement("DELETE FROM entries WHERE k=?");
            if (missingLegacyWal && meta("ready") != null
                    && !meta("sources").endsWith(":" + EMPTY_HASH)) {
                throw new IOException("Legacy WAL was removed outside SQLite; refusing to discard imported records");
            }
            if (missingLegacyWal) {
                Files.createFile(legacyWal);
            }
            recoverExport();
            String fingerprint = fingerprint();
            if (meta("ready") == null || !fingerprint.equals(meta("sources"))) {
                if (meta("ready") != null && !Objects.equals(meta("revision"), meta("exported"))) {
                    throw new IllegalStateException("SQLite and the legacy files both changed; refusing to "
                            + "overwrite metadata. Preserve both stores and reconcile them before restarting: " + database);
                }
                if (meta("ready") != null && !Files.exists(snapshot)) {
                    throw new IllegalStateException("Legacy snapshot was removed outside SQLite: " + snapshot);
                }
                importLegacy(fingerprint);
            }
            LOG.infov("Opened SQLite metadata index {0}, cache {1} KiB", database, cacheKiB);
        } catch (IOException | SQLException | RuntimeException exception) {
            release();
            throw failure("open", exception);
        }
    }

    private void importLegacy(String fingerprint) throws IOException {
        backup(snapshot);
        backup(legacyWal);
        transaction(() -> {
            execute("DELETE FROM entries");
            new LegacyStoreReader<>(mapper, type).read(snapshot, legacyWal, this::write, this::erase);
            if (!fingerprint.equals(fingerprint())) {
                throw new IOException("Legacy metadata changed during import; stop the other writer first");
            }
            setMeta("ready", "1");
            setMeta("sources", fingerprint);
            setMeta("revision", "0");
            setMeta("exported", "0");
        });
        LOG.infov("Imported legacy metadata into {0}; original files and backups are retained", database);
    }

    private static void backup(Path file) throws IOException {
        Path backup = file.resolveSibling(file.getFileName() + ".before-sqlite");
        if (Files.exists(file) && !Files.exists(backup)) {
            Files.copy(file, backup, StandardCopyOption.COPY_ATTRIBUTES);
        }
    }

    @Override
    public synchronized void put(String key, V value) {
        mutate(() -> write(key, value));
    }

    @Override
    public synchronized void putAll(Map<String, V> entries) {
        if (!entries.isEmpty()) {
            mutate(() -> entries.forEach(this::write));
        }
    }

    @Override
    public void putAllAtomically(Map<String, V> entries) {
        putAll(entries);
    }

    @Override
    public synchronized void delete(String key) {
        mutate(() -> erase(key));
    }

    @Override
    public synchronized void deleteAllAtomically(Set<String> keys) {
        if (!keys.isEmpty()) {
            mutate(() -> keys.forEach(this::erase));
        }
    }

    @Override
    public synchronized void clear() {
        mutate(() -> execute("DELETE FROM entries"));
    }

    private void mutate(Step step) {
        ensureReady();
        transaction(() -> {
            // Preserve the activity marker used by existing offline cleanup guards, without
            // appending legacy records or rewriting the snapshot on every request.
            if (!Files.exists(legacyWal)) {
                Files.createFile(legacyWal);
            }
            Files.setLastModifiedTime(legacyWal, FileTime.from(Instant.now()));
            step.run();
            setMeta("revision", Long.toString(Long.parseLong(meta("revision")) + 1));
        });
    }

    private void write(String key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        Index index = Objects.requireNonNull(projection.project(key, value));
        try {
            upsert.setBytes(1, bytes(key));
            upsert.setBytes(2, mapper.writeValueAsBytes(value));
            upsert.setString(3, index.scope());
            upsert.setBytes(4, bytes(index.name()));
            upsert.setString(5, index.version());
            upsert.setLong(6, -index.modified().getEpochSecond());
            upsert.setInt(7, -index.modified().getNano());
            upsert.setBytes(8, reverse(index.sequence()));
            upsert.setBytes(9, versionOrder(index.version()));
            upsert.setInt(10, index.kind());
            upsert.setInt(11, index.deleted() ? 1 : 0);
            upsert.executeUpdate();
        } catch (IOException | SQLException exception) {
            throw failure("write", exception);
        }
    }

    private void erase(String key) {
        try {
            remove.setBytes(1, bytes(Objects.requireNonNull(key)));
            remove.executeUpdate();
        } catch (SQLException exception) {
            throw failure("delete", exception);
        }
    }

    @Override
    public synchronized Optional<V> get(String key) {
        ensureReady();
        try (PreparedStatement statement = connection.prepareStatement("SELECT payload FROM entries WHERE k=?")) {
            statement.setBytes(1, bytes(key));
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(decode(rows.getBytes(1))) : Optional.empty();
            }
        } catch (SQLException exception) {
            throw failure("read", exception);
        }
    }

    @Override
    public synchronized List<V> scan(Predicate<String> filter) {
        ensureReady();
        List<V> result = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT k,payload FROM entries ORDER BY k")) {
            while (rows.next()) {
                if (filter.test(text(rows.getBytes(1)))) {
                    result.add(decode(rows.getBytes(2)));
                }
            }
            return result;
        } catch (SQLException exception) {
            throw failure("scan", exception);
        }
    }

    @Override
    public Set<String> keys() {
        return new AbstractSet<>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<>() {
                    private Iterator<String> batch = List.<String>of().iterator();
                    private String after;
                    private boolean end;

                    @Override
                    public boolean hasNext() {
                        if (!batch.hasNext() && !end) {
                            List<String> next = keyPage(after);
                            end = next.size() < KEY_PAGE;
                            batch = next.iterator();
                        }
                        return batch.hasNext();
                    }

                    @Override
                    public String next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        after = batch.next();
                        return after;
                    }
                };
            }

            @Override
            public int size() {
                return count();
            }
        };
    }

    private synchronized int count() {
        ensureReady();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM entries")) {
            return rows.next() ? rows.getInt(1) : 0;
        } catch (SQLException exception) {
            throw failure("count", exception);
        }
    }

    private synchronized List<String> keyPage(String after) {
        ensureReady();
        List<String> result = new ArrayList<>();
        String query = "SELECT k FROM entries" + (after == null ? "" : " WHERE k>?") + " ORDER BY k LIMIT " + KEY_PAGE;
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            if (after != null) {
                statement.setBytes(1, bytes(after));
            }
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.add(text(rows.getBytes(1)));
                }
            }
            return result;
        } catch (SQLException exception) {
            throw failure("page keys", exception);
        }
    }

    @Override
    public synchronized List<Entry<V>> page(String scope, String prefix, Cursor after, int limit, View view) {
        ensureReady();
        if (limit < 1 || limit > 10_000) {
            throw new IllegalArgumentException("SQLite page size must be between 1 and 10000");
        }
        byte[] lower = bytes(prefix);
        byte[] upper = successor(lower);
        byte[] cursorName = after == null ? null : bytes(after.name());
        boolean entryCursor = after != null && after.position() == Position.ENTRY
                && Arrays.compareUnsigned(cursorName, lower) >= 0;
        boolean exclusive = after != null && after.position() == Position.NAME
                && Arrays.compareUnsigned(cursorName, lower) >= 0;
        if (after != null && after.position() == Position.PREFIX) {
            byte[] bound = successor(cursorName);
            if (bound == null) {
                return List.of();
            }
            if (Arrays.compareUnsigned(bound, lower) > 0) {
                lower = bound;
            }
        } else if (exclusive || entryCursor) {
            lower = cursorName;
        }
        if (upper != null && Arrays.compareUnsigned(lower, upper) >= 0) {
            return List.of();
        }
        StringBuilder query = new StringBuilder("SELECT e.* FROM entries e WHERE e.scope=?");
        List<Object> parameters = new ArrayList<>(List.of(scope));
        // Give SQLite one lower range bound. Redundant name predicates can make its planner
        // seek from the prefix and filter every skipped row instead of using the cursor.
        if (entryCursor) {
            query.append(" AND (e.name,e.t,e.n,e.s,e.r,e.k)>(?,?,?,?,?,?)");
            parameters.add(cursorName);
            parameters.add(-after.index().modified().getEpochSecond());
            parameters.add(-after.index().modified().getNano());
            parameters.add(reverse(after.index().sequence()));
            parameters.add(versionOrder(after.index().version()));
            parameters.add(bytes(after.storageKey()));
        } else {
            query.append(exclusive ? " AND e.name>?" : " AND e.name>=?");
            parameters.add(lower);
        }
        if (upper != null) {
            query.append(" AND e.name<?");
            parameters.add(upper);
        }
        if (view == View.CURRENT) {
            query.append(" AND e.kind<>2 AND e.deleted=0 AND NOT EXISTS(SELECT 1 FROM entries d "
                    + "WHERE d.scope=e.scope AND d.name=e.name AND d.kind<>2 AND d.k<e.k)");
        } else if (view == View.VERSIONS || view == View.HISTORY) {
            query.append(view == View.HISTORY ? " AND e.kind=2" : " AND e.kind<>0");
            query.append(" AND NOT EXISTS(SELECT 1 FROM entries d WHERE d.scope=e.scope "
                    + "AND d.name=e.name AND d.version=e.version AND (d.kind>e.kind OR (d.kind=e.kind AND d.k<e.k)))");
        } else {
            query.append(" AND NOT EXISTS(SELECT 1 FROM entries d WHERE d.scope=e.scope AND d.name=e.name "
                    + "AND d.k<e.k AND ((d.kind<>2 AND e.kind<>2) OR (d.kind=2 AND e.kind=2 AND d.version=e.version)))");
        }
        query.append(" ORDER BY e.name,e.t,e.n,e.s,e.r,e.k LIMIT ?");
        parameters.add(limit);
        return query(query.toString(), parameters);
    }

    @Override
    public synchronized Optional<Entry<V>> version(String scope, String name, String version) {
        ensureReady();
        List<Entry<V>> result = query("SELECT * FROM entries WHERE scope=? AND name=? AND version=? "
                + "AND kind<>0 ORDER BY kind DESC,k LIMIT 1", List.of(scope, bytes(name), version));
        return result.stream().findFirst();
    }

    private List<Entry<V>> query(String query, List<Object> parameters) {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.get(i));
            }
            List<Entry<V>> result = new ArrayList<>();
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    V value = decode(rows.getBytes("payload"));
                    String key = text(rows.getBytes("k"));
                    result.add(new Entry<>(key, value, projection.project(key, value)));
                }
            }
            return result;
        } catch (SQLException exception) {
            throw failure("query index", exception);
        }
    }

    @Override
    public synchronized void flush() {
        if (connection == null) {
            return;
        }
        if (failed) {
            throw new IllegalStateException("SQLite transaction recovery failed; restart before exporting metadata");
        }
        try {
            recoverExport();
            if (!fingerprint().equals(meta("sources"))) {
                throw new IOException("Legacy metadata changed while SQLite was running; refusing to overwrite it");
            }
            if (!Objects.equals(meta("revision"), meta("exported")) || !Files.exists(snapshot)
                    || Files.size(legacyWal) != 0) {
                String newHash = exportSnapshot();
                transaction(() -> {
                    setMeta("export.json", newHash);
                    setMeta("export.old_json", fileHash(snapshot));
                    setMeta("export.old_wal", fileHash(legacyWal));
                    setMeta("export.revision", meta("revision"));
                });
                recoverExport();
            }
            execute("PRAGMA wal_checkpoint(TRUNCATE)");
        } catch (IOException | SQLException exception) {
            throw failure("export snapshot", exception);
        }
    }

    private String exportSnapshot() throws IOException, SQLException {
        MessageDigest digest = digest();
        try (OutputStream file = Files.newOutputStream(exportFile);
             DigestOutputStream output = new DigestOutputStream(file, digest);
             JsonGenerator generator = mapper.createGenerator(output);
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT k,payload FROM entries ORDER BY k")) {
            generator.writeStartObject();
            while (rows.next()) {
                generator.writeFieldName(text(rows.getBytes(1)));
                generator.writeRawValue(text(rows.getBytes(2)));
            }
            generator.writeEndObject();
        }
        try (FileChannel file = FileChannel.open(exportFile, StandardOpenOption.WRITE)) {
            file.force(true);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private void recoverExport() throws SQLException, IOException {
        String pending = meta("export.json");
        if (pending == null) {
            return;
        }
        if (!Objects.equals(meta("revision"), meta("export.revision"))) {
            throw new IOException("Metadata changed during an unfinished snapshot export");
        }
        String actualJson = fileHash(snapshot);
        String actualWal = fileHash(legacyWal);
        if (!actualWal.equals(meta("export.old_wal")) && !actualWal.equals(EMPTY_HASH)) {
            throw new IOException("Legacy WAL changed during snapshot export");
        }
        if (!actualJson.equals(pending)) {
            if (!actualJson.equals(meta("export.old_json"))) {
                throw new IOException("Legacy snapshot changed during snapshot export");
            }
            if (!Files.exists(exportFile) || !fileHash(exportFile).equals(pending)) {
                // No source changed. The database remains authoritative; retry the export later.
                transaction(this::clearExport);
                return;
            }
            Files.move(exportFile, snapshot, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        try (FileChannel wal = FileChannel.open(legacyWal, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            wal.force(true);
        }
        transaction(() -> {
            setMeta("sources", fingerprint());
            setMeta("exported", meta("revision"));
            clearExport();
        });
    }

    private void clearExport() throws SQLException {
        execute("DELETE FROM meta WHERE k LIKE 'export.%'");
    }

    private String fingerprint() throws IOException {
        return fileHash(snapshot) + ":" + fileHash(legacyWal);
    }

    private static String fileHash(Path file) throws IOException {
        if (!Files.exists(file)) {
            return "missing";
        }
        MessageDigest digest = digest();
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[1024 * 1024];
            for (int length; (length = input.read(buffer)) != -1;) {
                digest.update(buffer, 0, length);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private void transaction(Step step) {
        try {
            connection.setAutoCommit(false);
            try {
                step.run();
                connection.commit();
            } catch (IOException | SQLException | RuntimeException | Error exception) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    exception.addSuppressed(rollbackFailure);
                    failed = true;
                }
                if (exception instanceof Error fatal) {
                    throw fatal;
                }
                throw failure("commit transaction", (Exception) exception);
            } finally {
                if (!failed) {
                    connection.setAutoCommit(true);
                }
            }
        } catch (SQLException exception) {
            failed = true;
            throw failure("finish transaction", exception);
        }
    }

    private String meta(String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT v FROM meta WHERE k=?")) {
            statement.setString(1, key);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    private void setMeta(String key, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO meta VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v")) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }

    private void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private V decode(byte[] payload) {
        try {
            return mapper.readValue(payload, valueType);
        } catch (IOException exception) {
            throw failure("decode metadata", exception);
        }
    }

    private void ensureReady() {
        if (connection == null || failed) {
            throw new IllegalStateException("SQLite metadata is not open: " + database);
        }
        try {
            if (meta("export.json") != null) {
                throw new IllegalStateException("A snapshot export is unfinished; flush or restart before writing metadata");
            }
        } catch (SQLException exception) {
            throw failure("read export state", exception);
        }
    }

    @Override
    public synchronized void close() {
        try {
            flush();
        } finally {
            release();
        }
    }

    private void release() {
        for (AutoCloseable resource : new AutoCloseable[] {upsert, remove, connection, lease, leaseChannel}) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception exception) {
                    LOG.warnv(exception, "Failed to close a SQLite metadata resource for {0}", database);
                }
            }
        }
        connection = null;
        lease = null;
        leaseChannel = null;
        upsert = null;
        remove = null;
        if (processLease != null) {
            OPEN_DATABASES.remove(processLease);
            processLease = null;
        }
    }

    private IllegalStateException failure(String operation, Exception cause) {
        return new IllegalStateException("Cannot " + operation + " SQLite metadata at " + database, cause);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static byte[] reverse(String value) {
        byte[] source = bytes(value == null ? "" : value);
        byte[] result = new byte[source.length + 1];
        for (int i = 0; i < source.length; i++) {
            result[i] = (byte) ~source[i];
        }
        result[source.length] = (byte) 0xff;
        return result;
    }

    private static byte[] versionOrder(String version) {
        return reverse("null".equals(version) ? null : version);
    }

    private static byte[] successor(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            if ((prefix[i] & 0xff) != 0xff) {
                byte[] result = Arrays.copyOf(prefix, i + 1);
                result[i]++;
                return result;
            }
        }
        return null;
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static String hash(byte[] bytes) {
        return HexFormat.of().formatHex(digest().digest(bytes));
    }

    @FunctionalInterface
    private interface Step {
        void run() throws IOException, SQLException;
    }
}
