package org.example.dbs;
import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;

public class AtomDB implements BenchmarkDBAdapter {
    private final String dbName;
    private final DB db;

    public AtomDB() throws Exception {
        dbName = "ATOMDB_" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        var opt = new DbOptions();
        db = new DBImpl(Path.of(dbName), opt);
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
        db.close();
        db.destroy();
        Files.walk(Path.of(dbName))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}