package org.g2n.atomdb.db;

import org.g2n.atomdb.compaction.Compactor;
import org.g2n.atomdb.constants.DBConstant;
import org.g2n.atomdb.constants.Operations;
import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.logs.WALManager;
import org.g2n.atomdb.mem.ImmutableMem;
import org.g2n.atomdb.mem.SkipListMemtable;
import org.g2n.atomdb.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.g2n.atomdb.search.Search;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class DBImpl implements DB{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Path dbPath;
    private final WALManager walManager;
    private final Compactor compactor;
    private final Search search;
    private final DbOptions options;
    private final DbComponentProvider dbComponentProvider;
    private SkipListMemtable memtable;
    private Table table;
    private FileLock dbProcessLocking;
    private FileChannel dbLockFileChannel;
    private boolean isClosed = false;

    public DBImpl(Path pathForDB, DbOptions dbOptions) throws Exception {
        this.dbComponentProvider = new DbComponentProvider(dbOptions);
        this.dbPath = pathForDB.resolve("ATOM_DB");
        Files.createDirectories(dbPath);
        acquireDBLock();
        this.walManager = new WALManager(dbPath, dbComponentProvider);
        this.memtable = new SkipListMemtable(dbOptions.memtableSize, dbOptions.comparator);
        this.table = new Table(dbPath, dbComponentProvider);
        this.search = new Search(table, dbComponentProvider);
        this.compactor = new Compactor(table, search, dbPath, dbComponentProvider);
        this.options = dbOptions;
        walManager.restore(this);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        ensureOpen();
        var kvUnit = new KVUnit(key, value);

        walManager.log(Operations.WRITE, kvUnit);
        memtable.put(kvUnit);

        if (memtable.isFull()){
            handleMemtableFull();
        }
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        ensureOpen();
        Objects.requireNonNull(key);
        var kvUnit = memtable.get(key);
        if (kvUnit == null) {
            kvUnit = search.findKey(key);
        }
        return kvUnit == null || kvUnit.isDeleted() ?  null : kvUnit.getValue();
    }

    @Override
    public void delete(byte[] key) throws Exception {
        ensureOpen();
        KVUnit kvUnit = new KVUnit(key);
        walManager.log(Operations.DELETE, kvUnit);
        memtable.delete(kvUnit);
    }

    private void handleMemtableFull() throws Exception {
        var immutableMem = ImmutableMem.of(memtable);

        compactor.persistLevel0(immutableMem);
        search.addSecondaryMemtable(immutableMem);

        walManager.rotateLog();
        memtable = new SkipListMemtable(options.memtableSize, options.comparator); // todo we can have more memtable

        compactor.tryCompaction(Level.LEVEL_ZERO);
    }

    private void acquireDBLock() throws IOException {
        var lockFilePath = dbPath.resolve(DBConstant.DB_LOCK_FILE);
        if (!Files.exists(lockFilePath)) {
            Files.createFile(lockFilePath);
        }
        try {
            this.dbLockFileChannel = FileChannel.open(lockFilePath, StandardOpenOption.WRITE);
            this.dbProcessLocking = this.dbLockFileChannel.tryLock();
        } catch (IOException e) {
            logger.error("Failed to acquire lock on database: {}", dbPath, e);
            throw e;
        }
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Database is closed.");
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed) {
            logger.warn("Database at {} is already closed.", dbPath);
            return;
        }
        isClosed = true;
        walManager.close();
        search.close();
        compactor.close();
        dbProcessLocking.release();
        dbLockFileChannel.close();
    }

    @Override
    public void destroy() throws IOException {
        logger.info("Destroying database at: {}", dbPath);
        try (var stream = Files.walk(this.dbPath)) {
            stream.sorted(java.util.Comparator.reverseOrder()) // Important: delete children before parents
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
}
