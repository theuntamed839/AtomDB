package org.example;

import org.example.dbs.AtomDB;
import org.example.dbs.BenchmarkDBAdapter;
import org.example.dbs.NativeLevelDB;
import org.example.dbs.PureJavaLevelDB;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class BenchmarkWriteState {

    @Param({"10000", "100000"})
    public int entryCount;

    @Param({"atomDB", "levelDB_native", "levelDB_pure_java"})
    private String dbType;

    private BenchmarkDBAdapter db;

    public Map<byte[], byte[]> entries;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        switch (dbType) {
            case "atomDB" -> db = new AtomDB();
            case "levelDB_native" -> db = new NativeLevelDB();
            case "levelDB_pure_java" -> db = new PureJavaLevelDB();
            default -> throw new IllegalArgumentException("Unknown db: " + dbType);
        }

        entries = new TreeMap<>(Arrays::compare);
        var random = new Random(1234567890L);
        for (int i = 0; i < entryCount; i++) {
            var key = new byte[50];
            var value = new byte[500];
            random.nextBytes(key);
            random.nextBytes(value);
            entries.put(key, value);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        db.closeAndDestroy();
    }

    @Benchmark
    public void writeBenchmark(Blackhole bh) throws Exception {
        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            db.put(entry.getKey(), entry.getValue());
        }
        bh.consume(entries.size());
    }

    public static void main(String[] args) throws Exception {
        var opt = new OptionsBuilder()
                .include(BenchmarkWriteState.class.getSimpleName())
                .forks(1)
                .warmupIterations(1)
                .measurementIterations(3)
                .threads(1)
                .build();

        new Runner(opt).run();
    }
}
/*
# Run complete. Total time: 00:03:46

REMEMBER: The numbers below are just data. To gain reusable insights, you need to follow up on
why the numbers are the way they are. Use profilers (see -prof, -lprof), design factorial
experiments, perform baseline and negative tests that provide experimental control, make sure
the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the domain experts.
Do not assume the numbers tell you what you want them to tell.

NOTE: Current JVM experimentally supports Compiler Blackholes, and they are in use. Please exercise
extra caution when trusting the results, look into the generated code to check the benchmark still
works, and factor in a small probability of new VM bugs. Additionally, while comparisons between
different JVMs are already problematic, the performance difference caused by different Blackhole
modes can be very significant. Please make sure you use the consistent Blackhole mode for comparisons.

Benchmark                                    (dbType)  (entryCount)  Mode  Cnt        Score         Error  Units
BenchmarkWriteState.writeBenchmark             atomDB         10000  avgt    3    80621.812 �  345260.530  us/op
BenchmarkWriteState.writeBenchmark     levelDB_native         10000  avgt    3    98818.970 �   69804.859  us/op
BenchmarkWriteState.writeBenchmark     levelDB_native        100000  avgt    3  2579214.557 � 2753151.431  us/op
BenchmarkWriteState.writeBenchmark  levelDB_pure_java         10000  avgt    3   122816.055 �  153395.393  us/op
BenchmarkWriteState.writeBenchmark  levelDB_pure_java        100000  avgt    3  2578436.617 � 4138351.171  us/op
 */