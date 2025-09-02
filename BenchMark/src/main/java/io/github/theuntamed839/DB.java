package io.github.theuntamed839;

import io.github.theuntamed839.dbs.*;

public enum DB {
    LEVELDB_JAVA,
    DATASTORE4J,
    LEVELDB_NATIVE,
    ROCKSDB;

    public BenchmarkDBAdapter getAdapter() throws Exception {
        return switch (this) {
            case LEVELDB_NATIVE -> new NativeLevelDBAdaptor();
            case LEVELDB_JAVA -> new PureJavaLevelDBAdaptor();
            case ROCKSDB -> new RocksDBAdaptor();
            case DATASTORE4J -> new DataStore4JAdaptor();
        };
    }
}
