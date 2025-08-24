package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED
 */
public class Benchmark {
    public static final long SEED = 1234;

    public static void main(String[] args) throws Exception {
//        benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB),60_000_00, 500, 50);
        benchmarkWithRandomKVBytesWithNoInMemoryHold(
                DBProvider.get(DB.LEVELDB), 20_000_00, 500, 50);
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
            map.forEach((key, value) -> {
                try {
                    if (i.get() % 10000 == 0) {
                        System.out.println("progress="+i);
                    }
                    i.getAndIncrement();
                    db.put(key, value);
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
        var rand = new Random(1234);
        Map<byte[], byte[]> map = new HashMap<>(totalEntryCount);
        for (int i = 0; i < totalEntryCount; i++) {
            var key = new byte[rand.nextInt(10, keyBytesLength.get())];
            var value = new byte[rand.nextInt(10, valueBytesLength.get())];
            rand.nextBytes(key); rand.nextBytes(value);
            map.put(key, value);
        }
        // end
        return map;
    }

    private static void benchmarkWithRandomKVBytesWithNoInMemoryHold(BenchmarkDBAdapter db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        int interval = 10000;
        var rand = new Random(SEED);
        var randomSeeds = new ArrayList<Long>();
        for (int i = 0; i < totalEntryCount/interval; i++) {
            randomSeeds.add(rand.nextLong());
        }
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            int countDown = randomSeeds.size();
            for (Long randomSeed : randomSeeds) {
                System.out.println("progress=" + (countDown--));
                rand = new Random(randomSeed);
                for (int i = 0; i < interval; i++) {
                    var key = new byte[keyBytesLength];
                    var value = new byte[valueBytesLength];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    db.put(key, value);
                }
            }
            endTime = System.nanoTime();

            writingTime = endTime - startTime;

            Collections.shuffle(randomSeeds);

            System.out.println("Reading... ");
//            Scanner scan = new Scanner(System.in);
//            scan.nextLine();
            startTime = System.nanoTime();
            countDown = randomSeeds.size();
            for (Long randomSeed : randomSeeds) {
                System.out.println("progress=" + (countDown--));
                rand = new Random(randomSeed);
                for (int i = 0; i < interval; i++) {
                    var key = new byte[keyBytesLength];
                    var value = new byte[valueBytesLength];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    if (Arrays.compare(value, db.get(key)) != 0) {
                        throw new RuntimeException("Value mismatch for key: " + Arrays.toString(key));
                    }
                }
            }
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised In mb="+(actualMemUsed)/(1024 * 1024));
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.closeAndDestroy();
        }
    }
}

/**
 * AtomDB
*         benchmarkWithRandomKVBytes(DBProvider.get(DB.ATOMDB),20_000_00, 500, 50);
 * writing time=19.9684539 , reading time=19.3050245
 * writing time=22.0252559 , reading time=15.4229418
 *
 * LevelDB 60000000
 * writing time=600.5900685 , reading time=346.6775644
 * memory utilised=1005701160
 * Number of threads: 4
 *
 * writing time=114.535569 , reading time=1446.8922146
 * memory utilised In mb=1382
 * Number of threads: 12
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=5488959
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=469131
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=20428
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=637
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=18
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * total searches 6000000
 * totalValuesFromCache 20824
 * successfulSearchCount 6000000
 * unsuccessfulSearchCount 0
 * Number of actually compactions: 154
 */;