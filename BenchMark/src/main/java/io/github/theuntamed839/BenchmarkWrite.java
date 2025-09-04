package io.github.theuntamed839;

import org.openjdk.jmh.annotations.*;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class BenchmarkWrite extends AbstractBenchmark{

    private Random random;

    @Setup(Level.Invocation)
    public void setup() throws Exception {
        super.initDB();
        this.random = new Random(SEED);
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Benchmark
    public void writeFixedSizeData(Blackhole bh) throws Exception {
        for (int i = 0; i < entryCount; i++) {
            var key = new byte[keySize];
            var value = new byte[valueSize];
            random.nextBytes(key);
            random.nextBytes(value);
            db.put(key, value);
            bh.consume(key);
            bh.consume(value);
        }
    }

    @Benchmark
    public void writeVariableSizeData(Blackhole bh) throws Exception {
        for (int i = 0; i < entryCount; i++) {
            var key = new byte[random.nextInt(10, keySize)];
            var value = new byte[random.nextInt(10, valueSize)];
            random.nextBytes(key);
            random.nextBytes(value);
            db.put(key, value);
            bh.consume(key);
            bh.consume(value);
        }
    }

    public static void main(String[] args) throws Exception {
        var opt = new OptionsBuilder()
                .include(MethodHandles.lookup().lookupClass().getName())
                .forks(3)
                .warmupIterations(3)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}