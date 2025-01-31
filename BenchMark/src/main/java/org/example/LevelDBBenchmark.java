package org.example;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import static org.fusesource.leveldbjni.JniDBFactory.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;

public class LevelDBBenchmark implements BenchmarkDB{
    private final String dbName;
    private final DB db;

    public LevelDBBenchmark() throws IOException {
        dbName = "LEVELDB_" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(new File(dbName), options);
        JniDBFactory.pushMemoryPool(1024 * 1024 * 1024);
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
        JniDBFactory.popMemoryPool();
        Files.walk(Path.of(dbName))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}