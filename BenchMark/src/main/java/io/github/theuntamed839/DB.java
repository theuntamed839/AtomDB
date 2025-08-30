package io.github.theuntamed839;

import io.github.theuntamed839.dbs.*;

public enum DB {
    LEVELDB_JAVA,
    ATOMDB,
    LEVELDB_NATIVE,
    ROCKSDB;

    public BenchmarkDBAdapter getAdapter() throws Exception {
        return switch (this) {
            case ATOMDB -> new AtomDBAdaptor();
            case LEVELDB_NATIVE -> new NativeLevelDBAdaptor();
            case LEVELDB_JAVA -> new PureJavaLevelDBAdaptor();
            case ROCKSDB -> new RocksDBAdaptor();
        };
    }
}
