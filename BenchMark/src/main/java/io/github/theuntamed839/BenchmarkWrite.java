package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class BenchmarkWrite {
    @Param({"100000", "10"}) // {"100_0000", "100_000_00"}
    private int entryCount;

    @Param({"50", "500"}) // {"50", "500"}
    private int keySize;

    private final int valueSize = 500;

    private final long seed = 1234567890L;

    public TreeMap<byte[], byte[]> entries;

    @Setup(Level.Trial)
    public void initializeData() throws Exception {
        System.out.println("Setting data..." + Thread.currentThread().getName());

        entries = new TreeMap<>(Arrays::compare);
        var random = new Random(seed);

        for (int i = 0; i < entryCount; i++) {
            var key = new byte[keySize];
            var value = new byte[valueSize];
            random.nextBytes(key);
            random.nextBytes(value);
            entries.put(key, value);
        }
    }

    @Param({"ATOMDB", "LEVELDB_JAVA", "LEVELDB_NATIVE"})
    private DB dbProvider;

    private BenchmarkDBAdapter db;

    @Setup(Level.Invocation)
    public void setup() throws Exception {
        System.out.println("Setting up DB and populating data..." + Thread.currentThread().getName());
        db = dbProvider.getAdapter();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws Exception {
        System.out.println("Tearing down DB..." + Thread.currentThread().getName());
        db.closeAndDestroy();
    }

    @Benchmark
    public void writeBenchmark(Blackhole bh) throws Exception {
        System.out.println("Starting write benchmark..." + Thread.currentThread().getName());
        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            db.put(entry.getKey(), entry.getValue());
        }
        bh.consume(entries.size());
    }

    public static void main(String[] args) throws Exception {
        var opt = new OptionsBuilder()
                .include(BenchmarkWrite.class.getSimpleName())
                .forks(3)
                .warmupIterations(3)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}