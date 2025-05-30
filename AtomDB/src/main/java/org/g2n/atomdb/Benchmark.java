package org.g2n.atomdb;

import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.g2n.atomdb.util.BytesConverter.bytes;

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

//    private static void benchmarkWithRandomLengthKVBytes(int totalEntryCount) throws Exception {
//        var dbName = "benchmarkWithRandomKVBytes";
//        var rand = new Random();
//        var map = getRandomKV(totalEntryCount, () -> rand.nextInt(50, 100), () -> rand.nextInt(50, 1000));
//
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(dbName, opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Writing... " + totalEntryCount);
//            startTime = System.nanoTime();
//            map.entrySet().forEach(each -> {
//                try {
//                    db.put(each.getKey(), each.getValue());
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
//
//            writingTime = endTime - startTime;
//
//            var list = new ArrayList<>(map.keySet());
//            Collections.shuffle(list);
//            System.out.println("Reading... ");
//            startTime = System.nanoTime();
//            list.forEach(each -> {
//                try {
//                    db.get(each);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
//
//            readingTime = endTime - startTime;
//            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            Files.walk(Path.of(dbName ))
//                    .sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .forEach(File::delete);
//        }
//    }

//    private static void benchmarkWithRandomKVBytes(Map<byte[], byte[]> map) throws Exception {
//        var dbName = "benchmarkWithRandomKVBytes";
//
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(dbName, opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Writing... " + map.size());
//            startTime = System.nanoTime();
//            AtomicInteger i = new AtomicInteger();
//            map.entrySet().forEach(each -> {
//                try {
//                    if (i.get() % 10000 == 0) {
//                        System.out.println("progress="+i);
//                    }
//                    i.getAndIncrement();
//                    db.put(each.getKey(), each.getValue());
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
//
//            writingTime = endTime - startTime;
//
//            var list = new ArrayList<>(map.keySet());
//            Collections.shuffle(list);
//
//
//            System.out.println("Reading... ");
//            startTime = System.nanoTime();
//            list.forEach(each -> {
//                try {
//                    db.get(each);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
//
//            readingTime = endTime - startTime;
//            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            Files.walk(Path.of(dbName ))
//                    .sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .forEach(File::delete);
//        }
//    }

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

//    private static void benchmarkRandomRead(String inputString, long totalEntryCount, String dbName) throws Exception {
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(dbName, opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Writing... " + totalEntryCount);
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                db.put(bytes(i + ""), bytes(i + "_" + inputString));
//            }
//            endTime = System.nanoTime();
//
//            writingTime = endTime - startTime;
//            System.out.println("Reading... ");
//            startTime = System.nanoTime();
//            List<Integer> integers = new ArrayList<>(IntStream.range(0, (int) totalEntryCount).boxed().toList());
//            Collections.shuffle(integers);
//            Collections.shuffle(integers);
//            for (int i : integers) {
//                db.get(bytes(i + ""));
//            }
//            endTime = System.nanoTime();
//
//            readingTime = endTime - startTime;
//            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            Files.walk(Path.of(dbName ))
//                    .sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .forEach(File::delete);
//        }
//    }
//
//    public static void initialTest(String inputString, long totalEntryCount) throws Exception {
//        var opt = new DbOptions();
//        var db = new DBImpl(Benchmark.class.getName() + "DB", opt);
//        var key = "somegood things";
//        System.out.println("compressed keysize around ="+Snappy.compress(bytes(key)).length);
//        System.out.println("compressed valueSize ="+Snappy.compress(bytes(inputString)).length);
//
//        try {
//            for (int i = 0; i < totalEntryCount; i++) {
//                if (i % 17 == 0 ) {
//                    db.put(bytes(key + i), bytes(inputString));
//                    continue;
//                }
//                db.put(bytes(i + ""), bytes(i + "_" + inputString));
//            }
//            for (int i = 0; i < totalEntryCount; i++) {
//                if (i % 17 == 0 ) {
//                    byte[] bytes = db.get(bytes(key + i));
//                    byte[] bytes1 = bytes(inputString);
//                    if (Arrays.compare(bytes, bytes1) != 0) {
//                        System.out.println("moye moye");
//                    }
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            db.destroy();
//        }
//    }
//
//    public static void benchmarkWriting(String inputString, long totalEntryCount) throws Exception {
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(Benchmark.class.getName() + "DB", opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Writing... " + totalEntryCount);
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                db.put(bytes(i + ""), bytes(i + "_" + inputString));
//            }
//            endTime = System.nanoTime();
//
//            writingTime = endTime - startTime;
//            System.out.println("writing time=" + writingTime);
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            db.destroy();
//        }
//    }
//
//    public static void benchmark(String inputString, long totalEntryCount) throws Exception {
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(Benchmark.class.getName() + "DB", opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Writing... " + totalEntryCount);
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                db.put(bytes(i + ""), bytes(i + "_" + inputString));
//            }
//            endTime = System.nanoTime();
//
//            writingTime = endTime - startTime;
//            System.out.println("Writing ="+ writingTime);
//            System.out.println("Reading... ");
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                System.out.println("reading="+i);
//                db.get(bytes(i + ""));
//            }
//            endTime = System.nanoTime();
//
//            readingTime = endTime - startTime;
//            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            Files.walk(Path.of(Benchmark.class.getName() + "DB" ))
//                    .sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .forEach(File::delete);
//        }
//    }
//
//    public static void correctnessCheck(String inputString, long totalEntryCount) throws Exception {
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(Benchmark.class.getName() + "DB", opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Writing... " + totalEntryCount);
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                db.put(bytes(i + ""), bytes(inputString));
//            }
//            endTime = System.nanoTime();
//
//            writingTime = endTime - startTime;
//            System.out.println("Writing ="+ writingTime);
//            System.out.println("Reading... ");
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                System.out.println("reading="+i);
//                byte[] bytes = db.get(bytes(i + ""));
//                if (bytes == null) {
//                    throw new RuntimeException("value is null for key+"+i);
//                }
//                if (Arrays.compare(bytes, bytes(inputString)) != 0) {
//                    throw new RuntimeException("value is not same for key+"+i);
//                }
//            }
//            endTime = System.nanoTime();
//
//            readingTime = endTime - startTime;
//            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            Files.walk(Path.of(Benchmark.class.getName() + "DB" ))
//                    .sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .forEach(File::delete);
//        }
//    }
//
//    public static void searchBenchMark(long totalEntryCount, String DBName) throws Exception {
//        System.out.println("Number of threads: " + Thread.activeCount());
//        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//        var opt = new DbOptions();
//        var db = new DBImpl(Benchmark.class.getName() + "DB", opt);
//        long startTime , endTime, readingTime, writingTime;
//        try {
//            System.out.println("Reading... ");
//            startTime = System.nanoTime();
//            for (int i = 0; i < totalEntryCount; i++) {
//                db.get(bytes(i + ""));
//            }
//            endTime = System.nanoTime();
//
//            readingTime = endTime - startTime;
//            System.out.println("reading time=" + (readingTime/1000_000_000.0));
//            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
//            long actualMemUsed=afterUsedMem-beforeUsedMem;
//            System.out.println("memory utilised="+actualMemUsed);
//            System.out.println("Number of threads: " + Thread.activeCount());
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            db.close();
//            //org.g2n.atomdb.db.destroy();
//        }
//    }
}

/**
 * AtomDB
 * writing time=37.5845095 , reading time=17.5277052
 * memory utilised=587544248
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=879691
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=108216
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=7176
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=361
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=7
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=3
 *
 * FireFlyDB
 * writing time=15.806817 , reading time=31.6533273
 * memory utilised=574612224
 * Number of threads: 2
 *
 * LEVELDB_NATIVE
 * writing time=58.6611247 , reading time=12.1251576
 * memory utilised=280273920
 * Number of threads: 4
 *
 * writing time=57.736059 , reading time=16.4925723
 * memory utilised=518450384
 * Number of threads: 2
 *
 *
 * for visualVM
 * -Dcom.sun.management.jmxremote
 *
 * 2000000
 * writing time=108.6126411 , reading time=90.6029001
 * memory utilised=118638456
 * Number of threads: 7
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1655217
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=300789
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=35216
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=3244
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=233
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=22
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 *
 * leveldb_native 2000000
 * writing time=125.913601401 , reading time=24.416386
 * memory utilised=1484927096
 * Number of threads: 4
 *
 * writing time=129.5320343 , reading time=25.524798
 * memory utilised=352363104
 * Number of threads: 4
 *
 * only with greatest
 * writing time=26.465456 , reading time=24.3141119
 * memory utilised=1125201984
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=923028
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=69562
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=2790
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=71
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=3
 *
 * only wuth smallest
 *writing time=19.9734101 , reading time=5.7376749
 * memory utilised=209858952
 * Number of threads: 8
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=901527
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=89127
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4606
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=181
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=7
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=4
 *
 *
 * writing time=16.2603766 , reading time=6.2203587
 * memory utilised=6774336
 * Number of threads: 8
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=904991
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=85877
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4384
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=183
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=13
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=3
 * numberOfFilesRequiredToSearch=8 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=9 numberOfTimesThisHappened=1
 *
 * with smallest
 * writing time=19.27763 , reading time=6.3016042
 * memory utilised=356002344
 * Number of threads: 8
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=913413
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=78318
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=3577
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=128
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=13
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=8 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=12 numberOfTimesThisHappened=1
 * Number of actually compactions: 123
 * LEVEL_ZERO has 8971004 files
 * LEVEL_ZERO has 2 files
 * LEVEL_ONE has 58239930 files
 * LEVEL_ONE has 13 files
 * LEVEL_TWO has 1030437336 files
 * LEVEL_TWO has 230 files
 * LEVEL_THREE has 76276184 files
 * LEVEL_THREE has 17 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 files
 * LEVEL_SEVEN has 0 files
 *
 * writing time=18.0510931 , reading time=6.1232601
 * memory utilised=76570200
 * Number of threads: 7
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=899948
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=90523
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4805
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=176
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=2
 * Number of actually compactions: 112
 * LEVEL_ZERO has 17941728 files
 * LEVEL_ZERO has 4 files
 * LEVEL_ONE has 101413704 files
 * LEVEL_ONE has 23 files
 * LEVEL_TWO has 1014144074 files
 * LEVEL_TWO has 226 files
 * LEVEL_THREE has 40384740 files
 * LEVEL_THREE has 9 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 files
 * LEVEL_SEVEN has 0 files
 *
 *
 * 200000 with biggest only
 * writing time=75.4470958 , reading time=90.3080328
 * memory utilised=258508008
 * Number of threads: 7
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1605619
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=344430
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=41096
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=3336
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=212
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=22
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=4
 * numberOfFilesRequiredToSearch=8 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=9 numberOfTimesThisHappened=2
 * Number of actually compactions: 204
 * LEVEL_ZERO has 26912388 files
 * LEVEL_ZERO has 6 files
 * LEVEL_ONE has 440752570 files
 * LEVEL_ONE has 99 files
 * LEVEL_TWO has 1319069200 files
 * LEVEL_TWO has 294 files
 * LEVEL_THREE has 560752306 files
 * LEVEL_THREE has 125 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 files
 * LEVEL_SEVEN has 0 files
 *
 * 200000 with smallest only
 * writing time=57.2181866 , reading time=79.6361075
 * memory utilised=454990240
 * Number of threads: 7
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1713911
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=256743
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=22393
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=1540
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=100
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=8
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=6
 * numberOfFilesRequiredToSearch=8 numberOfTimesThisHappened=8
 * numberOfFilesRequiredToSearch=9 numberOfTimesThisHappened=5
 * numberOfFilesRequiredToSearch=10 numberOfTimesThisHappened=3
 * numberOfFilesRequiredToSearch=11 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=13 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=15 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=19 numberOfTimesThisHappened=1
 * Number of actually compactions: 225
 * LEVEL_ZERO has 8971144 files
 * LEVEL_ZERO has 2 files
 * LEVEL_ONE has 331271024 files
 * LEVEL_ONE has 74 files
 * LEVEL_TWO has 1330745466 files
 * LEVEL_TWO has 297 files
 * LEVEL_THREE has 676320826 files
 * LEVEL_THREE has 151 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 files
 * LEVEL_SEVEN has 0 files
 *
 *
 * With seed, 1000000
 * writing time=21.2621825 , reading time=6.7343162
 * memory utilised=782652920
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=905025
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=85476
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4736
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=205
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=7
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=9 numberOfTimesThisHappened=1
 * Number of actually compactions: 122
 * LEVEL_ZERO has 8971044 size in bytes
 * LEVEL_ZERO has 2 files
 * LEVEL_ONE has 49284874 size in bytes
 * LEVEL_ONE has 11 files
 * LEVEL_TWO has 743275768 size in bytes
 * LEVEL_TWO has 166 files
 * LEVEL_THREE has 372312118 size in bytes
 * LEVEL_THREE has 83 files
 * LEVEL_FOUR has 0 size in bytes
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 size in bytes
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 size in bytes
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 size in bytes
 * LEVEL_SEVEN has 0 files
 */