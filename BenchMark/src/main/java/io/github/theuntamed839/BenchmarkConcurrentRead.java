package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@BenchmarkMode({Mode.Throughput})
@State(Scope.Benchmark)
public class BenchmarkConcurrentRead {
    public int entryCount = 20_000_00; // todo
    private int keySize = 500;
    private final int valueSize = 50;
    private final long seed = 1234567890L;

    @Param({"ATOMDB", "LEVELDB_JAVA"}) // "LEVELDB_JAVA", "ATOMDB", "LEVELDB_NATIVE"
    private DB dbProvider;

    private BenchmarkDBAdapter db;

    public List<byte[]> keys;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        System.out.println("Setting up DB and populating data..." + Thread.currentThread().getName());
        db = dbProvider.getAdapter();
//        keys = fillDB(db, entryCount, keySize, valueSize, seed);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        System.out.println("Tearing down DB..." + Thread.currentThread().getName());
        db.closeAndDestroy();
//        System.gc(); // levelDB java implementation errors out otherwise
    }

    @Benchmark
    @Threads(Threads.MAX)
    public void concurrentSearch(Blackhole bh) throws Exception {
        byte[] key = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
        bh.consume(db.get(key));
        bh.consume(123);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BenchmarkConcurrentRead.class.getSimpleName())
                .forks(3)
                .warmupIterations(3)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}
