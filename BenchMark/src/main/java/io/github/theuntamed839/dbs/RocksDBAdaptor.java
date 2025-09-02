package io.github.theuntamed839.dbs;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class RocksDBAdaptor implements BenchmarkDBAdapter {
    static {
        RocksDB.loadLibrary();
    }
    private final RocksDB db;
    private final Path dbPath;

    public RocksDBAdaptor() throws IOException, RocksDBException {
        dbPath = Files.createDirectory(Path.of(this.getClass().getSimpleName() + "_" + UUID.randomUUID()));
        Options options = new Options();
        options.setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath.toString());
    }

    @Override
    public void put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws IOException {
        try {
            db.close();
            System.out.println(dbPath + " Folder size: " + getDirectorySize(dbPath));
        } finally {
            deleteDirectory(dbPath);
        }
    }
}