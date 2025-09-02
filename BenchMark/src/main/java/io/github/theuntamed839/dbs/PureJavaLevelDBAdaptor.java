package io.github.theuntamed839.dbs;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class PureJavaLevelDBAdaptor implements BenchmarkDBAdapter {
    private final DB db;
    private final Path dbPath;

    public PureJavaLevelDBAdaptor() throws IOException {
        dbPath = Files.createDirectory(Path.of(this.getClass().getSimpleName() + "_" + UUID.randomUUID()));
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(dbPath.toFile(), options);
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
            System.out.println(dbPath + " Folder size: " + getDirectorySize(dbPath));
        } finally {
            deleteDirectory(dbPath);
        }
    }
}