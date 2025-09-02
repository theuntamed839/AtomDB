package io.github.theuntamed839;

import org.openjdk.jmh.annotations.*;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode({Mode.SingleShotTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class BenchmarkRead extends AbstractBenchmarkRead {

    @Setup(Level.Trial)
    public void setup() throws Exception {
        super.initAndPopulateDB();
        super.makeShuffledKeys();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        super.tearDown();
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