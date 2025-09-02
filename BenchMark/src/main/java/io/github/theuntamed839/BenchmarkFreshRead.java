package io.github.theuntamed839;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.SingleShotTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class BenchmarkFreshRead extends AbstractBenchmarkRead {

    @Setup(Level.Invocation)
    public void setup() throws Exception {
        super.initAndPopulateDB();
        super.makeShuffledKeys();
    }

    @TearDown(Level.Invocation)
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