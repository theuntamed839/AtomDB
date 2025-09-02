package io.github.theuntamed839.dbs;

import io.github.theuntamed839.datastore4j.db.DB;
import io.github.theuntamed839.datastore4j.db.DataStore4J;
import io.github.theuntamed839.datastore4j.db.DbOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class DataStore4JAdaptor implements BenchmarkDBAdapter {
    private final Path dbPath;
    private final DB db;

    public DataStore4JAdaptor() throws Exception {
        dbPath = Files.createDirectory(Path.of(this.getClass().getSimpleName() + "_" + UUID.randomUUID()));
        var opt = new DbOptions();
        db = new DataStore4J(dbPath, opt);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws Exception {
        try {
            db.close();
            System.out.println(dbPath + " Folder size: " + getDirectorySize(dbPath));
            db.destroy();
        } finally {
            deleteDirectory(dbPath);
        }
    }
}