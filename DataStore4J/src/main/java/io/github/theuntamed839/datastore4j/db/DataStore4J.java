package io.github.theuntamed839.datastore4j.db;

import io.github.theuntamed839.datastore4j.compaction.Compactor;
import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.constants.Operations;
import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.wal.WALManager;
import io.github.theuntamed839.datastore4j.mem.ImmutableMem;
import io.github.theuntamed839.datastore4j.mem.SkipListMemtable;
import io.github.theuntamed839.datastore4j.table.Table;
import io.github.theuntamed839.datastore4j.search.Search;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.lang.System.Logger;

public class DataStore4J implements DB, AutoCloseable{
    private final Logger logger = System.getLogger(DataStore4J.class.getName());
    private final Path dbPath;
    private final WALManager walManager;
    private final Compactor compactor;
    private final Search search;
    private final DbComponentProvider dbComponentProvider;
    private SkipListMemtable memtable;
    private final Table table;
    private FileLock dbProcessLocking;
    private FileChannel dbLockFileChannel;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final StampedLock stampedLock = new StampedLock();
    private final LongAdder successfulSearchCount = new LongAdder();
    private final LongAdder unsuccessfulSearchCount = new LongAdder();

    public DataStore4J(Path pathForDB, DbOptions dbOptions) throws Exception {
        this.dbComponentProvider = new DbComponentProvider(dbOptions);

        this.dbPath = pathForDB.resolve("DATASTORE4J");
        Files.createDirectories(dbPath);
        acquireDBLock();
        this.walManager = new WALManager(dbPath, dbComponentProvider);
        this.memtable = new SkipListMemtable(dbComponentProvider.getMemtableSize(), dbComponentProvider.getComparator());
        this.search = new Search(dbComponentProvider);
        this.table = new Table(dbPath, search, dbComponentProvider);
        this.compactor = new Compactor(table, search, dbPath, dbComponentProvider);
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

            if(kvUnit == null) {
                unsuccessfulSearchCount.increment();
                return null;
            }

            successfulSearchCount.increment();

            if (kvUnit.isTombStone()) {
                return null;
            } else {
                return kvUnit.getValue();
            }
        }finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public SearchStats getSearchStats() {
        ensureOpen();
        return new SearchStats(search.getReaderStats(), successfulSearchCount.longValue(), unsuccessfulSearchCount.longValue());
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
            memtable = new SkipListMemtable(dbComponentProvider.getMemtableSize(), dbComponentProvider.getComparator());

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
            logger.log(Logger.Level.INFO, String.format("Destroying database: %s", dbPath));
            try (var stream = Files.walk(this.dbPath)) {
                stream.sorted(java.util.Comparator.reverseOrder())
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
            dbLockFileChannel = FileChannel.open(lockFilePath, StandardOpenOption.WRITE);
            FileLock lock = dbLockFileChannel.tryLock();
            if (lock != null) {
                dbProcessLocking = lock;
                logger.log(Logger.Level.INFO, String.format("Lock acquired on database file: %s", lockFilePath));
            } else {
                throw new IOException("Database is already opened by another process: " + dbPath);
            }
        } catch (Exception e) {
            logger.log(Logger.Level.ERROR, String.format("Failed to acquire database lock %s: %s", lockFilePath, e.getMessage()));
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
            if (isClosed.get()) {
                logger.log(Logger.Level.WARNING, String.format("Database at %s is already closed.", dbPath));
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
