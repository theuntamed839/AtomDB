package org.example.dbs;


import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;

public class RocksDBAdaptor implements BenchmarkDBAdapter {
    static {
        RocksDB.loadLibrary();
    }
    private final RocksDB db;
    private final Path dbPath;

    public RocksDBAdaptor() throws IOException, RocksDBException {
        dbPath = Files.createTempDirectory("LEVELDB_NATIVE_" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        Options options = new Options();
        options.createIfMissing();
        db = RocksDB.open(options, dbPath.toString());
    }

    @Override
    public void put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException, RocksDBException {
        return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws IOException {
        db.close();
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}