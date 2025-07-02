package org.example;

import org.example.intervalsAlgo.*;
import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;
import java.util.*;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class IntervalAlgoBenchmark {

    public static final int SEED = 1234;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IntervalAlgoBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(3)
                .forks(1)
                .threads(4)
                .build();

        new Runner(opt).run();
    }

    @Param({
            "LinearIntervalSearch",
            "IntervalTree",
            "IntervalTreeRedBlack",
            "JavaTreeBasedIntervalSearch",
            "TimeLineIntervalSearch",
            "TimeLineIntervalSearchJavaBinarySearch"
    })
    public String algoType;

//    @Param({"100", "1000", "1000"})
    public int fillSize = 1000;

    @Param({"10000", "100000", "1000000"})
    public int searchSize;

    private IntervalAlgo algo;

    private List<byte[]> searchKeys;

    @Setup(Level.Trial)
    public void setup() {
        algo = getAlgo();
        Random random = new Random(SEED);

        fill(algo, random, fillSize);

        searchKeys = new ArrayList<>();
        for (int i = 0; i < searchSize; i++) {
            byte[] l1 = new byte[random.nextInt(10, 500)];
            random.nextBytes(l1);
            searchKeys.add(l1);
        }
    }

    private static void fill(IntervalAlgo algo, Random random, int size) {
        for (int i = 0; i < size; i++) {
            byte[] l1 = new byte[random.nextInt(10, 500)];
            byte[] l2 = new byte[random.nextInt(10, 500)];
            random.nextBytes(l1); random.nextBytes(l2);

            if (Arrays.compare(l1, l2) >= 0) continue;

            Interval interval = new Interval(l1, l2, UUID.randomUUID().toString());
            algo.insert(interval);
        }
    }

    private IntervalAlgo getAlgo() {
        return switch (algoType) {
            case "LinearIntervalSearch" -> new LinearIntervalSearch();
            case "IntervalTree" -> new IntervalTree();
            case "IntervalTreeRedBlack" -> new IntervalTreeRedBlack();
            case "JavaTreeBasedIntervalSearch" -> new JavaTreeBasedIntervalSearch(); // your algorithm
            case "TimeLineIntervalSearch" -> new TimeLineIntervalSearch();
            case "TimeLineIntervalSearchJavaBinarySearch" -> new TimeLineIntervalSearchJavaBinarySearch();
            default -> throw new RuntimeException("Not found");
        };
    }

    @Benchmark
    public int testGetIntervals() {
        int total = 0;
        for (byte[] key : searchKeys) {
            total += algo.getIntervals(key).size();
        }
        return total;
    }
}

