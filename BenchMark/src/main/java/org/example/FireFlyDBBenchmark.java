package org.example;

import com.sahilbondre.firefly.FireflyDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;

public class FireFlyDBBenchmark implements BenchmarkDB {

    private final FireflyDB db;
    private final String dbName;

    public FireFlyDBBenchmark() throws IOException {
        dbName = "fireflyDB" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        Files.createDirectories(Paths.get(dbName));
        db = FireflyDB.getInstance(dbName);
        db.start();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        db.set(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws IOException {
        db.stop();
        Files.walk(Paths.get(dbName))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}
