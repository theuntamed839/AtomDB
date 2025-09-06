package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;
import org.openjdk.jmh.annotations.*;

import java.util.List;

import static io.github.theuntamed839.Util.fillDB;

@State(Scope.Benchmark)
public abstract class AbstractBenchmark {

    protected static final long SEED = 1234567890L; // consistent seed

    @Param({"1000000", "5000000", "10000000"})
    public int entryCount;

    @Param({"DATASTORE4J", "ROCKSDB", "LEVELDB_JAVA", "LEVELDB_NATIVE"})
    public DB dbProvider;

    @Param({"500"})
    public int keySize;

    @Param({"500"})
    public int valueSize;

    protected BenchmarkDBAdapter db;
    protected List<byte[]> keys;
    protected List<byte[]> shuffledKeys;

    protected void initDB() throws Exception {
        db = dbProvider.getAdapter();
    }

    protected void initAndPopulateDB() throws Exception {
        db = dbProvider.getAdapter();
        keys = fillDB(db, entryCount, keySize, valueSize, SEED);
    }

    protected void initAndPopulateDBWithVariableSizeData() throws Exception {
        db = dbProvider.getAdapter();
        keys = fillDB(db, entryCount, keySize, 10, valueSize, 10, SEED);
    }

    protected void makeShuffledKeys() {
        shuffledKeys = new java.util.ArrayList<>(keys);
        java.util.Collections.shuffle(shuffledKeys, new java.util.Random(SEED));
    }

    protected void tearDown() throws Exception {
        db.closeAndDestroy();
        keys = null;
        shuffledKeys = null;
        db = null;
    }
}
