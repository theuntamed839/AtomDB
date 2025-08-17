package org.example.dbs;

import com.sleepycat.je.*;
import java.io.File;

public class BerkeleyDBAdapter implements BenchmarkDBAdapter {
    private Environment env;
    private Database db;
    private File dir;

    public BerkeleyDBAdapter() throws Exception {
        this.dir = new File("BERKELEYDB_" + System.currentTimeMillis());
        this.dir.mkdir();
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(dir, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        db = env.openDatabase(null, "testdb", dbConfig);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        DatabaseEntry k = new DatabaseEntry(key);
        DatabaseEntry v = new DatabaseEntry(value);
        db.put(null, k, v);
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        DatabaseEntry k = new DatabaseEntry(key);
        DatabaseEntry v = new DatabaseEntry();
        if (db.get(null, k, v, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            return v.getData();
        }
        return null;
    }

    @Override
    public void closeAndDestroy() throws Exception {
        db.close();
        env.close();
        for (File f : dir.listFiles()) {
            f.delete();
        }
        dir.delete();
    }
}
