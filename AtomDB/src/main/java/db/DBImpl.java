package db;

import Compaction.Compactor;
import Constants.Operations;
import Level.Level;
import Logs.WALManager;
import Mem.ImmutableMem;
import Mem.SkipListMemtable;
import Table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import search.Search;
import util.Util;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class DBImpl implements DB{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final File dbFolder;
    private final WALManager walManager;
    private final Compactor compactor;
    private final Search search;
    private final DbOptions options;
    private SkipListMemtable memtable;
    private Table table;

    public DBImpl(File dbFolder, DbOptions dbOptions) throws Exception {
        createDB(dbFolder);
        this.dbFolder = dbFolder;
        this.walManager = new WALManager(dbFolder.getAbsolutePath());
        this.memtable = new SkipListMemtable(dbOptions);
        this.search = new Search();
        this.table = new Table(dbFolder, search);
        this.compactor = new Compactor(table, dbOptions);
        this.options = dbOptions;
        walManager.restore(this);
    }

    private void createDB(File dbFolder) {
        if (dbFolder.isDirectory() || dbFolder.mkdirs()) {
            new File(dbFolder, "ATOM_DB");
        } else {
            throw new RuntimeException("Unable to create db folder");
        }
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

    private void handleMemtableFull() throws Exception {
        var immutableMem = ImmutableMem.of(memtable);

        compactor.persistLevelFile(immutableMem);
        search.addSecondaryMemtable(immutableMem);

        walManager.rotateLog();
        memtable = new SkipListMemtable(options); // todo we can have more memtable

        compactor.tryCompaction(Level.LEVEL_ZERO);
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        Objects.requireNonNull(key);
        var kvUnit = memtable.get(key);
        if (kvUnit == null) {
            kvUnit = search.findKey(key);
        }
        return kvUnit.isDeleted() ? null : kvUnit.getValue();
    }

    @Override
    public void delete(byte[] key) throws Exception {
        KVUnit kvUnit = new KVUnit(key);
        walManager.log(Operations.DELETE, kvUnit);
        memtable.delete(kvUnit);
    }

    @Override
    public void close() throws Exception {
        try {
            walManager.close();
            search.close();
            compactor.close();
        } catch (IOException e) {
            logger.error("Failed to close resources: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void destroy() {
        validateFolder(dbFolder);
        deleteFolderContents(dbFolder);
    }

    private void validateFolder(File folder) {
        Util.requireTrue(folder.exists(), "Folder " + folder.getPath() + " does not exist");
        Util.requireTrue(folder.isDirectory(), "File " + folder.getPath() + " is not a folder");
    }

    private void deleteFolderContents(File folder) {
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (!file.delete()) {
                logger.warn("Unable to delete file: {}", file.getAbsolutePath());
            }
        }
        if (!folder.delete()) {
            logger.warn("Unable to delete folder: {}", folder.getPath());
        }
    }
}
