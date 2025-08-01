package org.example;

import org.example.dbs.BenchmarkDBAdapter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        benchmarkWithRandomKVBytes(DBProvider.get(DB.ATOMDB),1000000, 50, 500);
    }

    private static void benchmarkWithRandomKVBytes(BenchmarkDBAdapter db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
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
        var rand = new Random(123456789L);
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