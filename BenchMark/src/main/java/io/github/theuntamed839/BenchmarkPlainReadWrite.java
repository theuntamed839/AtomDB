package io.github.theuntamed839;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.SingleShotTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class BenchmarkPlainReadWrite extends AbstractBenchmark {

    private Random random;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        super.initDB();
        this.random = new Random(SEED);
        this.shuffledKeys = new ArrayList<>(entryCount);
    }

    @TearDown(Level.Trial)
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
            shuffledKeys.add(key);
        }
    }

    @Benchmark
    public void randomSearch(Blackhole bh) throws Exception {
        Collections.shuffle(shuffledKeys, random);
        for (byte[] key : shuffledKeys) {
            bh.consume(db.get(key));
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