package db;

import Compaction.Compactor;
import Compression.CompressionStrategyFactory;
import Compression.DataCompressionStrategy;
import Constants.DBConstant;
import Constants.Operations;
import Logs.WALManager;
import Mem.ImmutableMem;
import Mem.SkipListMemtable;
import Table.Table;
import sst.SSTManager;
import util.Util;

import java.io.File;
import java.util.Objects;

// todo shrink some value to its native size, like some places long is used
// even though that thing is int

// todo make all bytebuffer direct

// todo change all arrays.compare to db comparator

// todo improve error messages

public class DBImpl implements DB{
    private final File dbFolder;
    private final DataCompressionStrategy compression;
    private final WALManager walManager;
    private final Compactor compactor;
    private SkipListMemtable memtable;
    private SSTManager sstManager;
    private Table table;
    private int MEMTABLE_SIZE = DBConstant.MEMTABLE_SIZE;

    public DBImpl(File dbFolder, DBOptions dbOptions) throws Exception {
        ValidateDbFolder(dbFolder);
        createDBFolder(dbFolder);
        this.dbFolder = dbFolder;
        this.compression = CompressionStrategyFactory.GetCompressionStrategy(dbOptions.isDisableCompression());
        this.walManager = new WALManager(dbFolder.getAbsolutePath());
        this.sstManager = new SSTManager(dbFolder);
        this.memtable = new SkipListMemtable(DBComparator.byteArrayComparator);
        this.compactor = new Compactor(dbFolder);
        walManager.restore(this);
    }

    private void ValidateDbFolder(File dbFolder) {
        Objects.requireNonNull(dbFolder, "DB name not provided");
        if (dbFolder.exists() && !dbFolder.isDirectory()) {
            throw new RuntimeException("File name provided is not directory");
        }
    }

    private void createDBFolder(File dbFolder) {
        if (dbFolder.isDirectory()) {
            // todo check if the folder is DB.
        } else {
            if (!dbFolder.mkdirs()) {
                throw new RuntimeException("Unable to create db folder");
            }
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        var kvUnit = new KVUnit(key, value);
        walManager.log(Operations.WRITE, kvUnit);
        memtable.put(kvUnit);
        if (memtable.getMemTableSize() >= MEMTABLE_SIZE) {
            compactor.persistLevelFile(ImmutableMem.of(memtable));
//            finder.acceptSecondaryMem();
            walManager.deleteOldLogAndCreateNewLog();
            memtable = new SkipListMemtable(DBComparator.byteArrayComparator);
        }
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        Objects.requireNonNull(key);
        // todo search engine.
        KVUnit kvUnit = memtable.get(key);
        if (kvUnit.getIsDelete() == KVUnit.DELETE) {
            return null;
        } else {
            return kvUnit.getValue();
        }
    }

    @Override
    public void delete(byte[] key) throws Exception {
        walManager.log(Operations.DELETE, new KVUnit(key, KVUnit.DELETE));
        memtable.delete(new KVUnit(key, KVUnit.DELETE));
    }


    @Override
    public void close() throws Exception {
        walManager.close();
        sstManager.close();

        // todo remove this and find bugs
        System.gc();
    }

    @Override
    public void destroy() {
        Util.requireTrue(dbFolder.exists(), "folder="+dbFolder.toPath()+" does not exits");
        Util.requireTrue(dbFolder.isDirectory(), "file="+dbFolder.toPath()+" is not a folder");

        for (File listFile : dbFolder.listFiles()) {
            Util.requireTrue(listFile.delete(), "unable to delete file="+listFile.getAbsolutePath());
        }
        Util.requireTrue(dbFolder.delete(), "unable to delete folder="+dbFolder.toPath());
    }
}
