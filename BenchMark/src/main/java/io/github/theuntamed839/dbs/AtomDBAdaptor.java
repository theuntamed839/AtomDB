package io.github.theuntamed839.dbs;
import io.github.theuntamed839.atomdb.db.AtomDB;
import io.github.theuntamed839.atomdb.db.DB;
import io.github.theuntamed839.atomdb.db.DbOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

public class AtomDBAdaptor implements BenchmarkDBAdapter {
    private final Path dbPath;
    private final DB db;
    private boolean isClosed = false;

    public AtomDBAdaptor() throws Exception {
        System.out.println("Initializing AtomDBAdaptor..." + Thread.currentThread().getName());
        dbPath = Files.createDirectory(Path.of(this.getClass().getSimpleName() + "_" + UUID.randomUUID()));
        var opt = new DbOptions();
        db = new AtomDB(dbPath, opt);
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
        System.out.println("Closing and destroying AtomDBAdaptor..." + Thread.currentThread().getName());
        if (isClosed) return;
        db.close();
        db.destroy();
        try (var stream = Files.walk(dbPath)) {
            stream.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        }
        isClosed = true;
    }
}