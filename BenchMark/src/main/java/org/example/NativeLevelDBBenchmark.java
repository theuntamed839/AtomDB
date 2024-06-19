package org.example;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class NativeLevelDBBenchmark implements BenchmarkDB{

    private final DB db;
    private final String dbName;

    public NativeLevelDBBenchmark() throws IOException {
        dbName = "LEVELDB_NATIVE_" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(new File(dbName), options);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws IOException {
        db.close();
        Files.walk(Path.of(dbName))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}