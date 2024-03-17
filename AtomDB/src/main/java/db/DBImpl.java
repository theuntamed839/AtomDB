package db;

import Compression.CompressionStrategyFactory;
import Compression.DataCompressionStrategy;
import Constants.Operations;
import Logs.WALManager;
import Mem.SkipListMemtable;
import sst.SSTManager;
import Table.Table;
import util.BytesConverter;
import util.Util;

import java.io.File;
import java.io.IOException;
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
    private SkipListMemtable memtable;
    private SSTManager sstManager;
    private Table table;

    public DBImpl(File dbFolder, DBOptions dbOptions) throws Exception {
        ValidateDbFolder(dbFolder);
        createDBFolder(dbFolder);
        this.dbFolder = dbFolder;

        this.compression = CompressionStrategyFactory.GetCompressionStrategy(dbOptions.isDisableCompression());

        this.walManager = new WALManager(dbFolder);

        this.sstManager = new SSTManager(dbFolder);
        this.memtable = new SkipListMemtable(dbFolder, DBComparator.byteArrayComparator, walManager, sstManager);
    }

    private void ValidateDbFolder(File dbFolder) {
        Objects.requireNonNull(dbFolder, "DB name not provided");
        if (dbFolder.exists() && !dbFolder.isDirectory()) {
            throw new RuntimeException("File name provided is not directory");
        }
    }

    // todo need to be removed
    public Table getTable() {
        return table;
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
        Objects.requireNonNull(key, "null not allowed");
        Objects.requireNonNull(value, "null not allowed");
        walManager.logOP(key, value, Operations.WRITE);
        memtable.put(compression.compress(key), compression.compress(value));
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        Objects.requireNonNull(key, "null not allowed");
        byte[] bytes = this.memtable.get(compression.compress(key));
        return compression.decompress(bytes);
    }

    @Override
    public byte[] delete(byte[] key) throws Exception {
        walManager.logOP(key,
                BytesConverter.bytes("DELETED_KEY_VALUE"),
                Operations.DELETE);
        return memtable.delete(compression.compress(key));
    }


    @Override
    public void close() throws Exception {
        walManager.close();
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
