package io.github.theuntamed839;

import io.github.theuntamed839.dbs.AtomDB;
import io.github.theuntamed839.dbs.BenchmarkDBAdapter;
import io.github.theuntamed839.dbs.NativeLevelDBAdaptor;
import io.github.theuntamed839.dbs.RocksDBAdaptor;
import io.github.theuntamed839.dbs.PureJavaLevelDBAdaptor;

public class DBProvider {
    public static BenchmarkDBAdapter get(DB db) throws Exception {
        return switch (db) {
            case LEVELDB -> new PureJavaLevelDBAdaptor();
            case LEVELDB_NATIVE -> new NativeLevelDBAdaptor();
            case ATOMDB -> new AtomDB();
            case ROCKSDB -> new RocksDBAdaptor();
        };
    }
}

