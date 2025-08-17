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
            case ROCKSDB -> new org.example.dbs.RocksDBAdaptor();
            case BERKELEYDB -> new org.example.dbs.BerkeleyDBAdapter();
            case CHRONICLEDB -> new org.example.dbs.ChronicleMapAdapter();
            case MAPDB -> new org.example.dbs.MapDBAdapter();
            case LMDB -> new org.example.dbs.LMDBAdapter();
        };
    }
}

enum DB {
    LEVELDB,
    ATOMDB,
    LEVELDB_NATIVE,
    ROCKSDB,
    BERKELEYDB,
    CHRONICLEDB,
    MAPDB,
    LMDB,
}