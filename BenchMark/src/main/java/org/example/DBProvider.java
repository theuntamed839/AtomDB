package org.example;

import org.example.dbs.AtomDB;
import org.example.dbs.BenchmarkDBAdapter;
import org.example.dbs.NativeLevelDB;
import org.example.dbs.PureJavaLevelDB;

public class DBProvider {
    public static BenchmarkDBAdapter get(DB db) throws Exception {
        return switch (db) {
            case LEVELDB -> new PureJavaLevelDB();
            case LEVELDB_NATIVE -> new NativeLevelDB();
            case ATOMDB -> new AtomDB();
        };
    }
}

enum DB {
    LEVELDB,
    ATOMDB,
    LEVELDB_NATIVE
}