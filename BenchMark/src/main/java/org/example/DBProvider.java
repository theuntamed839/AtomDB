package org.example;

import java.io.IOException;

public class DBProvider {
    public static BenchmarkDB get(DB db) throws IOException {
        return switch (db) {
            case LEVELDB -> new LevelDBBenchmark();
            case FIREFLYDB -> new FireFlyDBBenchmark();
            case LEVELDB_NATIVE -> new NativeLevelDBBenchmark();
        };
    }
}

enum DB {
    LEVELDB,
    FIREFLYDB,
    LEVELDB_NATIVE
}