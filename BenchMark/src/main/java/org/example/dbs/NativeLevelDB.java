package org.example.dbs;

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

public class NativeLevelDB implements BenchmarkDBAdapter {
    private final Path dbPath;
    private final DB db;

    public NativeLevelDB() throws IOException {
        dbPath = Files.createTempDirectory("LEVELDB_" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(dbPath.toFile(), options);
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
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}