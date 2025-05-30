package org.g2n.atomdb.db;

import org.g2n.atomdb.Compaction.Compactor;
import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Constants.Operations;
import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Logs.WALManager;
import org.g2n.atomdb.Mem.ImmutableMem;
import org.g2n.atomdb.Mem.SkipListMemtable;
import org.g2n.atomdb.Table.Table;
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

    public DBImpl(Path pathForDB, DbOptions dbOptions) throws Exception {
        this.dbComponentProvider = new DbComponentProvider(dbOptions);
        this.dbPath = pathForDB.resolve("ATOM_DB");
        Files.createDirectories(dbPath);
        acquireDBLock();
        this.walManager = new WALManager(dbPath, dbComponentProvider);
        this.memtable = new SkipListMemtable(dbOptions.memtableSize, dbOptions.comparator);
        this.search = new Search(dbComponentProvider);
        this.table = new Table(dbPath, search, dbComponentProvider);
        this.table.fillLevels(); // Load existing levels from the database folder
        this.compactor = new Compactor(table, dbPath, dbComponentProvider);
        this.options = dbOptions;
        walManager.restore(this);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        var kvUnit = new KVUnit(key, value);

        walManager.log(Operations.WRITE, kvUnit);
        memtable.put(kvUnit);

        if (memtable.isFull()){
            handleMemtableFull();
        }
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        Objects.requireNonNull(key);
        var kvUnit = memtable.get(key);
        if (kvUnit == null) {
            kvUnit = search.findKey(key);
        }
        return kvUnit == null || kvUnit.isDeleted() ?  null : kvUnit.getValue();
    }

    @Override
    public void delete(byte[] key) throws Exception {
        KVUnit kvUnit = new KVUnit(key);
        walManager.log(Operations.DELETE, kvUnit);
        memtable.delete(kvUnit);
    }

    @Override
    public void close() throws Exception {
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
}
