package db;

import Mem.MemtableManager;
import sst.SSTManager;
import Table.Table;
import util.Util;

import java.io.File;
import java.io.IOException;

// todo shrink some value to its native size, like some places long is used
// even though that thing is int

// todo make all bytebuffer direct

// todo change all arrays.compare to db comparator
public class DBImpl implements DB{
    private MemtableManager memtable;
    private SSTManager sstManager;
    private Table table;
    private DBOptions dbOptions;

    public DBImpl(DBOptions dbOptions) throws Exception {
        this.dbOptions = dbOptions;

        createDBFolder(dbOptions);

        this.table = new Table(dbOptions);
        this.sstManager = new SSTManager(dbOptions, table);
        this.memtable = new MemtableManager(dbOptions, sstManager);
        this.memtable.start(this);
    }

    // todo need to be reomoved
    public Table getTable() {
        return table;
    }

    private void createDBFolder(DBOptions dbOptions) throws IOException {
        File dbFolder = new File(dbOptions.getDBfolder());
        if (!dbFolder.isDirectory()) {
            if (!dbFolder.mkdirs()) {
                throw new IOException("unable to create db folder");
            }
        }
    }

    @Override
    public byte[] put(byte[] key, byte[] value) throws Exception {
        return this.memtable.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        return this.memtable.get(key);
    }

    @Override
    public byte[] delete(byte[] key) throws Exception {
        return this.memtable.delete(key);
    }


    @Override
    public void close() throws IOException {
        this.memtable.close();
    }

    @Override
    public void destroy() {
        var file = new File(this.dbOptions.getDBfolder());
        Util.requireTrue(file.exists(), "folder="+file.toPath()+" does not exits");
        Util.requireTrue(file.isDirectory(), "file="+file.toPath()+" is not a folder");

        for (File listFile : file.listFiles()) {
            Util.requireTrue(listFile.delete(), "unable to delete file="+listFile.toPath());
        }
        Util.requireTrue(file.delete(), "unable to delete folder="+file.toPath());
    }
}
