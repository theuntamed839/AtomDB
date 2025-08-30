package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static io.github.theuntamed839.Util.fillDB;

/*
BenchmarkRead -f 3 -wi 3 -i 3 -t 1
 */

@BenchmarkMode({Mode.SingleShotTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class BenchmarkRead {

    @Param({"2000000"}) // {"100_0000", "100_000_00"
    public int entryCount;

    @Param({"500"}) // {"50", "500"}
    private int keySize;

    private final int valueSize = 50;

    private final long seed = 1234567890L;

    @Param({"ATOMDB", "LEVELDB_JAVA", "LEVELDB_NATIVE"}) // "LEVELDB_JAVA", "ATOMDB", "LEVELDB_NATIVE"
    private DB dbProvider;

    private BenchmarkDBAdapter db;

    public List<byte[]> keys;

    private List<byte[]> shuffledKeys;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        db = dbProvider.getAdapter();
        keys = fillDB(db, entryCount, keySize, valueSize, seed);
        shuffledKeys = new ArrayList<>(keys);
        Collections.shuffle(shuffledKeys, new Random(seed));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        db.closeAndDestroy();
    }

    @Benchmark
    public void linearSearch(Blackhole bh) throws Exception {
        for (byte[] key : keys) {
            bh.consume(db.get(key));
        }
    }

    @Benchmark
    public void randomSearch(Blackhole bh) throws Exception {
        for (byte[] key : shuffledKeys) {
            bh.consume(db.get(key));
        }
    }

    @Benchmark
    public void reverseSearch(Blackhole bh) throws Exception {
        for (int i = keys.size() - 1; i >= 0; i--) {
            bh.consume(db.get(keys.get(i)));
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BenchmarkRead.class.getSimpleName())
                .forks(3)
                .warmupIterations(3)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}