package org.g2n.atomdb.db;

import org.g2n.atomdb.compaction.Compactor;
import org.g2n.atomdb.constants.DBConstant;
import org.g2n.atomdb.constants.Operations;
import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.wal.WALManager;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

public class DBImpl implements DB, AutoCloseable{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Path dbPath;
    private final WALManager walManager;
    private final Compactor compactor;
    private final Search search;
    private final DbOptions options;
    private final DbComponentProvider dbComponentProvider;
    private SkipListMemtable memtable;
    private final Table table;
    private FileLock dbProcessLocking;
    private FileChannel dbLockFileChannel;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final StampedLock stampedLock = new StampedLock();

    public DBImpl(Path pathForDB, DbOptions dbOptions) throws Exception {
        this.dbComponentProvider = new DbComponentProvider(dbOptions);
        this.dbPath = pathForDB.resolve("ATOM_DB");
        Files.createDirectories(dbPath);
        acquireDBLock();
        this.walManager = new WALManager(dbPath, dbComponentProvider);
        this.memtable = new SkipListMemtable(dbOptions.memtableSize, dbOptions.getComparator());
        this.search = new Search(dbComponentProvider);
        this.table = new Table(dbPath, search, dbComponentProvider);
        this.compactor = new Compactor(table, search, dbPath, dbComponentProvider);
        this.options = dbOptions;
        walManager.restore(this);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        var kvUnit = new KVUnit(key, value);
        writeUnit(kvUnit, Operations.WRITE);
    }

    @Override
    public void delete(byte[] key) throws Exception {
        KVUnit kvUnit = new KVUnit(key);
        writeUnit(kvUnit, Operations.DELETE);
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        long stamp = stampedLock.readLock();
        try {
            ensureOpen();
            Objects.requireNonNull(key);
            var kvUnit = memtable.get(key);
            if (kvUnit == null) {
                kvUnit = search.findKey(key);
            }
            return kvUnit == null || kvUnit.isDeleted() ? null : kvUnit.getValue();
        }finally {
            stampedLock.unlockRead(stamp);
        }
    }

    private void writeUnit(KVUnit kvUnit, Operations operations) throws Exception {
        long stamp = stampedLock.writeLock();
        try {
            ensureOpen();
            walManager.log(operations, kvUnit);
            memtable.put(kvUnit);

            if (!memtable.isFull()) {
                return;
            }

            var immutableMem = ImmutableMem.of(memtable);

            compactor.persistLevel0(immutableMem);
            search.addSecondaryMemtable(immutableMem);

            walManager.rotateLog();
            memtable = new SkipListMemtable(options.memtableSize, options.getComparator());

            compactor.tryCompaction(Level.LEVEL_ZERO);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    public void destroy() throws IOException {
        long stamp = stampedLock.writeLock();
        try {
            if (!isClosed.get()) {
                throw new IllegalStateException("Database must be closed before destroying.");
            }
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
        }finally {
            stampedLock.unlockWrite(stamp);
        }
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
        if (isClosed.get()) {
            throw new IllegalStateException("DB is already closed.");
        }
    }

    @Override
    public void close() throws Exception {
        long stamp = stampedLock.writeLock();
        try {
            ensureOpen();
            if (isClosed.get()) {
                logger.warn("Database at {} is already closed.", dbPath);
                return;
            }
            isClosed.set(true);
            walManager.close();
            search.close();
            compactor.close();
            dbProcessLocking.release();
            dbLockFileChannel.close();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }
}
