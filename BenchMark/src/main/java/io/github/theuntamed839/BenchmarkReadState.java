package io.github.theuntamed839;

import io.github.theuntamed839.dbs.AtomDB;
import io.github.theuntamed839.dbs.BenchmarkDBAdapter;
import io.github.theuntamed839.dbs.NativeLevelDBAdaptor;
import io.github.theuntamed839.dbs.PureJavaLevelDBAdaptor;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class BenchmarkReadState {

    @Param({"10000", "100000"})
    public int entryCount;

    @Param({"atomDB", "levelDB_native", "levelDB_pure_java"})
    private String dbType;

    private BenchmarkDBAdapter db;

    public List<byte[]> keys;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        switch (dbType) {
            case "atomDB" -> db = new AtomDB();
            case "levelDB_native" -> db = new NativeLevelDBAdaptor();
            case "levelDB_pure_java" -> db = new PureJavaLevelDBAdaptor();
            default -> throw new IllegalArgumentException("Unknown db: " + dbType);
        }

        keys = new ArrayList<>(entryCount);
        var random = new Random(1234567890L);
        for (int i = 0; i < entryCount; i++) {
            var key = new byte[50];
            var value = new byte[500];
            random.nextBytes(key);
            random.nextBytes(value);

            keys.add(key);
            db.put(key, value);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        db.closeAndDestroy();
    }

    @Benchmark
    public void linearSearch(Blackhole bh) throws Exception {
        for (byte[] key : keys) {
            bh.consume(db.get(key));
        }
    }

    @Benchmark
    public void randomSearch(Blackhole bh) throws Exception {
        var shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled);
        for (byte[] key : shuffled) {
            bh.consume(db.get(key));
        }
    }

    @Benchmark
    public void reverseSearch(Blackhole bh) throws Exception {
        for (int i = keys.size() - 1; i >= 0; i--) {
            bh.consume(db.get(keys.get(i)));
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BenchmarkReadState.class.getSimpleName())
                .forks(1)
                .warmupIterations(1)
                .measurementIterations(3)
                .threads(1)
                .build();

        new Runner(opt).run();
    }
}
/*

# Run complete. Total time: 00:25:34

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

Benchmark                                  (dbType)  (entryCount)   Mode  Cnt       Score         Error   Units
BenchmarkReadState.linearSearch              atomDB         10000  thrpt    3      ? 10?�                ops/us
BenchmarkReadState.linearSearch              atomDB        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.linearSearch      levelDB_native         10000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.linearSearch      levelDB_native        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.linearSearch   levelDB_pure_java         10000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.linearSearch   levelDB_pure_java        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.randomSearch              atomDB         10000  thrpt    3      ? 10?�                ops/us
BenchmarkReadState.randomSearch              atomDB        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.randomSearch      levelDB_native         10000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.randomSearch      levelDB_native        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.randomSearch   levelDB_pure_java         10000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.randomSearch   levelDB_pure_java        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.reverseSearch             atomDB         10000  thrpt    3      ? 10?�                ops/us
BenchmarkReadState.reverseSearch             atomDB        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.reverseSearch     levelDB_native         10000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.reverseSearch     levelDB_native        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.reverseSearch  levelDB_pure_java         10000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.reverseSearch  levelDB_pure_java        100000  thrpt    3      ? 10??                ops/us
BenchmarkReadState.linearSearch              atomDB         10000   avgt    3    2787.675 �    3184.182   us/op
BenchmarkReadState.linearSearch              atomDB        100000   avgt    3  217698.842 �  193807.713   us/op
BenchmarkReadState.linearSearch      levelDB_native         10000   avgt    3   15905.009 �   12385.133   us/op
BenchmarkReadState.linearSearch      levelDB_native        100000   avgt    3  925572.601 � 1663748.402   us/op
BenchmarkReadState.linearSearch   levelDB_pure_java         10000   avgt    3   28392.340 �   35266.524   us/op
BenchmarkReadState.linearSearch   levelDB_pure_java        100000   avgt    3  431507.430 �  428765.460   us/op
BenchmarkReadState.randomSearch              atomDB         10000   avgt    3    2926.395 �    2153.941   us/op
BenchmarkReadState.randomSearch              atomDB        100000   avgt    3  220982.453 �  331698.788   us/op
BenchmarkReadState.randomSearch      levelDB_native         10000   avgt    3   16404.125 �   24516.902   us/op
BenchmarkReadState.randomSearch      levelDB_native        100000   avgt    3  787054.036 �  246137.343   us/op
BenchmarkReadState.randomSearch   levelDB_pure_java         10000   avgt    3   24297.482 �    2768.232   us/op
BenchmarkReadState.randomSearch   levelDB_pure_java        100000   avgt    3  388519.170 �  140424.764   us/op
BenchmarkReadState.reverseSearch             atomDB         10000   avgt    3    2413.706 �      40.186   us/op
BenchmarkReadState.reverseSearch             atomDB        100000   avgt    3  179565.580 �   59327.345   us/op
BenchmarkReadState.reverseSearch     levelDB_native         10000   avgt    3   13071.956 �    3064.076   us/op
BenchmarkReadState.reverseSearch     levelDB_native        100000   avgt    3  766880.136 �  151049.919   us/op
BenchmarkReadState.reverseSearch  levelDB_pure_java         10000   avgt    3   23469.881 �    3395.982   us/op
BenchmarkReadState.reverseSearch  levelDB_pure_java        100000   avgt    3  374578.783 �   46467.923   us/op
 */