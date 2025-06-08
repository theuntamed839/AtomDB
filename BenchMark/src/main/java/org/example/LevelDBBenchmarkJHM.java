package org.example;

import org.iq80.leveldb.*;
import org.iq80.leveldb.DB;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.fusesource.leveldbjni.JniDBFactory.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class LevelDBBenchmarkJHM {

    private DB db;
    private byte[] sampleKey;
    private byte[] sampleValue;
    private static final int VALUE_SIZE = 500;

    public static void main(String[] args) throws RunnerException {
        var options = new OptionsBuilder()
                .include(LevelDBBenchmarkJHM.class.getSimpleName())
                .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Options options = new Options();
        options.createIfMissing(true);

        db = factory.open(new File("leveldb_jmh"), options);
        sampleKey = bytes("sample_key");
        sampleValue = bytes("val_" + "x".repeat(VALUE_SIZE - 4));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        db.close();
        factory.destroy(new File("leveldb_jmh"), new Options());
    }

    @org.openjdk.jmh.annotations.Benchmark
    public void benchmarkPut() {
        String key = "key_" + UUID.randomUUID();
        db.put(bytes(key), sampleValue);
    }

    @org.openjdk.jmh.annotations.Benchmark
    public void benchmarkGet() {
        db.get(sampleKey);  // should be present after setup
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
