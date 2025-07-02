package org.g2n.atomdb;

import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(5);
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(() -> {
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            long[] threadIds = bean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.

            if (threadIds != null) {
                ThreadInfo[] infos = bean.getThreadInfo(threadIds);

                for (ThreadInfo info : infos) {
                    System.out.println("Thread " + info.getThreadName() + " is deadlocked.");
                    StackTraceElement[] stack = info.getStackTrace();
                    for (StackTraceElement element : stack) {
                        System.out.println("\t" + element);
                    }
                    // Log or store stack trace information.
                }
            }
        });

        System.out.println("Warm Up with 50k");
        //searchBenchMark(500000, "benchmarkWithRandomKVBytesWithCompaction");
        //searchBenchMark(500000, "benchmarkWithRandomKVBytesWithoutCompaction");
        //searchBenchMark(500000, "IssueDB");

//        benchmark(inputString, 500000);
//        correctnessCheck(inputString, 500000);

        //        benchmark(inputString, 1000);
//        benchmark(inputString, 10000);
//        benchmark(inputString, 100000);
//        benchmark(inputString, 1000_000);
//        benchmarkWriting(inputString, 1000_000);
//        initialTest(inputString, 50000);
//                benchmark(inputString, 15000);
//        benchmarkWithRandomKVBytes(1000000, 50, 500); //500000
        benchmarkWithRandomKVBytes(1000000, 50, 500);

//        benchmarkWithRandomKVBytes(getRandomKV(1000000, () -> 50, () -> 500));

//        benchmarkWithRandomLengthKVBytes(1000_000);
//        benchmarkRandomRead(inputString, 1000_000, "asd"); //1000000
        executorService.shutdown();
        executorService.shutdownNow();
        executorService.close();
    }

    static String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }

    private static void benchmarkWithRandomKVBytes(int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        var map = getRandomKV(totalEntryCount, () -> keyBytesLength, () -> valueBytesLength);
        var opt = new DbOptions();
        var dbName = Path.of("benchmarkWithRandomKVBytes_" + getSaltString());
        opt.disallowUseOfMMap();
        var db = new DBImpl(dbName, opt);
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
                        System.out.println("progress=" + i);
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
//            Scanner scan = new Scanner(System.in);
//            scan.nextLine();
            startTime = System.nanoTime();
            list.forEach(each -> {
                try {
                    db.get(each);
//                    System.out.println(Arrays.toString(db.get(each)));
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
            db.close();
            Files.walk(dbName)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
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
/**
 * not print statements.
 * benchmarkWithRandomKVBytes(1000000, 50, 500);
 *
 * writing time=18.2794853 , reading time=5.7827799
 * memory utilised=628613312
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=914646
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=76940
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=3730
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=127
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=11
 * Number of actually compactions: 115
 * LEVEL_ZERO has 8971044 size in bytes
 * LEVEL_ZERO has 2 files
 * LEVEL_ONE has 31262940 size in bytes
 * LEVEL_ONE has 7 files
 * LEVEL_TWO has 837585186 size in bytes
 * LEVEL_TWO has 187 files
 * LEVEL_THREE has 296066634 size in bytes
 * LEVEL_THREE has 66 files
 * LEVEL_FOUR has 0 size in bytes
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 size in bytes
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 size in bytes
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 size in bytes
 * LEVEL_SEVEN has 0 files
 */