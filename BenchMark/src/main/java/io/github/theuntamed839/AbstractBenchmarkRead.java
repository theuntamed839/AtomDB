package io.github.theuntamed839;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;


public abstract class AbstractBenchmarkRead extends AbstractBenchmark{
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
}
