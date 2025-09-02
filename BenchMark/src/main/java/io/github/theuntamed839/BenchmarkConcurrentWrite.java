package io.github.theuntamed839;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ThreadLocalRandom;

@BenchmarkMode({Mode.Throughput})
@State(Scope.Benchmark)
public class BenchmarkConcurrentWrite extends AbstractBenchmark{

    @Setup(Level.Trial)
    public void setup() throws Exception {
        super.initAndPopulateDB();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Benchmark
    @Threads(Threads.MAX)
    public void prepopulatedConcurrentWrite(Blackhole bh) throws Exception {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        byte[] key = new byte[keySize];
        byte[] value = new byte[valueSize];
        rand.nextBytes(key);
        rand.nextBytes(value);
        db.put(key, value);
        bh.consume(key);
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