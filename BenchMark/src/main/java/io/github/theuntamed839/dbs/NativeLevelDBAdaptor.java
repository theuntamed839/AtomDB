package io.github.theuntamed839.dbs;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import static org.fusesource.leveldbjni.JniDBFactory.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class NativeLevelDBAdaptor implements BenchmarkDBAdapter {
    private final Path dbPath;
    private final DB db;

    public NativeLevelDBAdaptor() throws IOException {
        dbPath = Files.createDirectory(Path.of(this.getClass().getSimpleName() + "_" + UUID.randomUUID()));
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(dbPath.toFile(), options);
        JniDBFactory.pushMemoryPool(1024 * 512);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) {
       return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws IOException {
        try {
            db.close();
            JniDBFactory.popMemoryPool();
            System.out.println(dbPath + " Folder size: " + getDirectorySize(dbPath));
        } finally {
            deleteDirectory(dbPath);
        }
    }
}