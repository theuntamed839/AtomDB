package org.example.intervalsAlgo;

import java.util.*;

public class Test {

    public static final int SEED = 42;

    public static void main(String[] args) {
//        compareResult(new LinearIntervalSearch(), new IntervalTreeRedBlack());
//        compareResult(new LinearIntervalSearch(), new TimeLineIntervalSearchJavaBinarySearch());

        measure(new LinearIntervalSearch(),1000,  1000, new Random(SEED));
        measure(new LinearIntervalSearch(), 10000, 10000, new Random(SEED));
        measure(new LinearIntervalSearch(), 10000, 100000, new Random(SEED));
        measure(new LinearIntervalSearch(), 1000, 1000_0000, new Random(SEED));
//        measure(new LinearIntervalSearch(), 10000, 1000_000, new Random(SEED));
//        System.out.println();

        measure(new TimeLineIntervalSearch(),1000,  1000, new Random(SEED));
        measure(new TimeLineIntervalSearch(), 10000, 10000, new Random(SEED));
        measure(new TimeLineIntervalSearch(), 10000, 100000, new Random(SEED));
        measure(new TimeLineIntervalSearch(), 1000, 1000_0000, new Random(SEED));
//        measure(new TimeLineIntervalSearch(), 10000, 1000_000, new Random(SEED));
//        System.out.println();
//        measure(new TimeLineIntervalSearchJavaBinarySearch(),1000,  1000, new Random(SEED));
//        measure(new TimeLineIntervalSearchJavaBinarySearch(), 10000, 10000, new Random(SEED));
//        measure(new TimeLineIntervalSearchJavaBinarySearch(), 10000, 100000, new Random(SEED));
//        measure(new TimeLineIntervalSearchJavaBinarySearch(), 1000, 1000_000, new Random(SEED));
//        measure(new TimeLineIntervalSearchJavaBinarySearch(), 10000, 1000_000, new Random(SEED));
//        System.out.println();
//
//        measure(new JavaTreeBasedIntervalSearch(),1000,  1000, new Random(SEED));
//        measure(new JavaTreeBasedIntervalSearch(), 10000, 10000, new Random(SEED));
//        measure(new JavaTreeBasedIntervalSearch(), 10000, 100000, new Random(SEED));
//        measure(new JavaTreeBasedIntervalSearch(), 1000, 1000_000, new Random(SEED));
//        measure(new JavaTreeBasedIntervalSearch(), 10000, 1000_000, new Random(SEED));
        System.out.println();

        measure(new IntervalTree(), 1000, 1000, new Random(SEED));
        measure(new IntervalTree(), 10000, 10000, new Random(SEED));
        measure(new IntervalTree(), 10000, 100000, new Random(SEED));
        measure(new IntervalTree(), 1000, 1000_0000, new Random(SEED));
//        measure(new IntervalTree(), 10000, 1000_000, new Random(SEED));
        System.out.println();

        measure(new IntervalTreeRedBlack(), 1000, 1000, new Random(SEED));
        measure(new IntervalTreeRedBlack(), 10000, 10000, new Random(SEED));
        measure(new IntervalTreeRedBlack(), 10000, 100000, new Random(SEED));
        measure(new IntervalTreeRedBlack(), 1000, 1000_0000, new Random(SEED));
//        measure(new IntervalTreeRedBlack(), 10000, 1000_000, new Random(SEED));
    }

    private static void measure(IntervalAlgo algo, int fillSize, int iteration, Random random) {
        var start1 = System.nanoTime();
        fill(algo, new Random(SEED), fillSize);
        var end1 = System.nanoTime();
        System.out.println(algo.getClass().getName() + " timeTaken to fill "+ (end1 - start1)/1000_000_000.0);

        var toSearchList = new ArrayList<byte[]>();
        for (int i = 0; i < iteration; i++) {
            byte[] l1 = new byte[random.nextInt(10, 500)];
            random.nextBytes(l1);
            toSearchList.add(l1);
        }

        start1 = System.nanoTime();
        toSearchList.forEach(algo::getIntervals);
        end1 = System.nanoTime();
        System.out.println(algo.getClass().getName() + " timeTaken to search"+ (end1 - start1)/1000_000_000.0);
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

    private static void compareResult(IntervalAlgo algo1, IntervalAlgo algo2) {
        fill(algo1, new Random(SEED), 10000);
        fill(algo2, new Random(SEED), 10000);
        test(algo1, algo2);
    }

    private static void test(IntervalAlgo algo1, IntervalAlgo algo2) {
        Random random = new Random(SEED);
        for (int i = 0; i < 10000; i++) {
            byte[] keyToFind = new byte[random.nextInt(10, 500)];
            random.nextBytes(keyToFind);

            List<Interval> result1 = algo1.getIntervals(keyToFind);
            List<Interval> result2 = algo2.getIntervals(keyToFind);
            System.out.println("Length " + result1.size() + " " + result2.size());

            if (result1.size() != result2.size()) {
                System.out.println("failed");
                break;
            }

            Collections.sort(result1);
            Collections.sort(result2);
            if (!result1.equals(result2)) {
                System.out.println("results mismatch");
                break;
            }
        }
    }
}
