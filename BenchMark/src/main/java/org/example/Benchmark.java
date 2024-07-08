package org.example;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB_NATIVE),1000000, 50, 500);
    }

    private static void benchmarkWithRandomKVBytes(BenchmarkDB db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        var map = getRandomKV(totalEntryCount, () -> keyBytesLength, () -> valueBytesLength);

        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            AtomicInteger i = new AtomicInteger();
            map.entrySet().forEach(each -> {
                try {
                    if (i.get() % 10000 == 0) {
                        System.out.println("progress="+i);
                    }
                    i.getAndIncrement();
                    db.put(each.getKey(), each.getValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            endTime = System.nanoTime();

            writingTime = endTime - startTime;

            var list = new ArrayList<>(map.keySet());
            Collections.shuffle(list);

//            Scanner s = new Scanner(System.in);
//            s.nextLine();

            System.out.println("Reading... ");
            startTime = System.nanoTime();
            list.forEach(each -> {
                try {
                    db.get(each);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.closeAndDestroy();
        }
    }

    private static Map<byte[], byte[]> getRandomKV(int totalEntryCount, Supplier<Integer> keyBytesLength, Supplier<Integer> valueBytesLength) {
        // total entries
        System.out.println("random generation");
        var rand = new Random();
        Map<byte[], byte[]> map = new HashMap<>(totalEntryCount);
        for (int i = 0; i < totalEntryCount; i++) {
            var key = new byte[keyBytesLength.get()];
            var value = new byte[valueBytesLength.get()];
            rand.nextBytes(key); rand.nextBytes(value);
            map.put(key, value);
        }
        // end
        return map;
    }
}

/**
 * Results
 * benchmarkWithRandomKVBytes(DBProvider.get(DB.FIREFLYDB),500000, 50, 500); FIRE
 * writing time=7.0718969 , reading time=12.3250532
 * memory utilised=373248264
 * Number of threads: 2
 *
 * benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB),500000, 50, 500); LEVEL
 * writing time=21.5763701 , reading time=8.2168154
 * memory utilised=325047808
 * Number of threads: 2
 * 7 compactions
 *
 * benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB_NATIVE),500000, 50, 500); NATIVE
 * writing time=24.8875308 , reading time=3.7821177
 * memory utilised=307651760
 * Number of threads: 4
 * 6-7 compactions
 *
 *
 * AtomDB for 500000
 * writing time=10.9193016 , reading time=3.3202372
 * 1000000
 * writing time=78.9897571 , reading time=27.1927732
 *
 * LevelDB 1000000
 * writing time=63.0487536 , reading time=23.4485624
 */