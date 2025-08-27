package io.github.theuntamed839.atomdb;
import io.github.theuntamed839.atomdb.db.AtomDB;
import io.github.theuntamed839.atomdb.db.DB;
import io.github.theuntamed839.atomdb.db.DbOptions;
import io.github.theuntamed839.atomdb.db.SearchStats;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * DBProvider.get(DB.ROCKSDB), 100_000_00, 500, 50);
 *
 * writing time=83.811962071 , reading time=196.47133151
 *
 * memory utilised In mb=121
 *
 * Number of threads: 2
 *
 * DBProvider.get(DB.LEVELDB_NATIVE), 100_000_00, 500, 50);
 *
 * writing time=835.058119744 , reading time=280.806907674
 *
 * memory utilised In mb=77
 *
 * Number of threads: 2
 *
 * DBProvider.get(DB.LEVELDB), 100_000_00, 500, 50);
 *
 * writing time=690.481524067 , reading time=772.416939435
 *
 * memory utilised In mb=1915
 *
 * Number of threads: 4
 *
 * AtomDB
 *
 * writing time=135.333601975 , reading time=208.431767516
 *
 * memory utilised In mb=1050
 *
 * Number of threads: 9
 *
 * 4096
 *
 * 4096
 *
 * writing time=230.258618308 , reading time=112.972731875
 *
 * memory utilised In mb=2663
 *
 * Number of threads: 2
 *
 * this.file = new File("MapDB_" + System.currentTimeMillis());
 * db = DBMaker
 *         .fileDB(file)
 *         .transactionEnable()
 *         .fileMmapEnable()
 *         .make();
 * map = db.hashMap("map", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
 *
 */

public class perf {

    public static final long SEED = 1234;
    public static void main(String[] args) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();

        System.out.println("Total Memory: " + totalMemory / (1024 * 1024) + " MB");
        System.out.println("Free Memory: " + freeMemory / (1024 * 1024) + " MB");
        System.out.println("Max Memory: " + maxMemory / (1024 * 1024) + " MB");
//        ExecutorService executorService = Executors.newCachedThreadPool();
//        executorService.execute(() -> {
//            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
//            long[] threadIds = bean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.
//
//            if (threadIds != null) {
//                ThreadInfo[] infos = bean.getThreadInfo(threadIds);
//
//                for (ThreadInfo info : infos) {
//                    System.out.println("Thread " + info.getThreadName() + " is deadlocked.");
//                    StackTraceElement[] stack = info.getStackTrace();
//                    for (StackTraceElement element : stack) {
//                        System.out.println("\t" + element);
//                    }
//                    // Log or store stack trace information.
//                }
//            }
//        });

        var opt = new DbOptions();
//        opt.disallowUseOfMMap();
//        opt.setSSTFileSizeAndMemTableSize(
//                DBConstant.COMPACTED_SST_FILE_SIZE,
//                DBConstant.COMPACTED_SST_FILE_SIZE
//        );
//        var jimfs = Jimfs.newFileSystem(Configuration.unix());
        var dbPath = Files.createDirectory(Path.of("benchmarkWithRandomKVBytes_" + Instant.now().toEpochMilli()));
//        var dbPath = Files.createTempDirectory(jimfs.getPath("/"), "benchmarkWithRandomKVBytes_" + Instant.now().toEpochMilli());
//        var dbPath = Files.createDirectory(Path.of("benchmarkWithRandomKVBytes_60_000_00"));
        var db = new AtomDB(dbPath, opt);

//        20_000_00


//        benchmarkWithRandomKVBytes(db, 20_000_00, 500, 50);
        benchmarkWithRandomKVBytesWithNoInMemoryHold(db, 100_000_00, 500, 50);
//        benchmarkWithRandomKVBytesWith3Reads(db, 20_000_00, 500, 50);

//        justRead(db,60_000_00, 500, 50);

        System.out.println(Files.size(dbPath));
//        System.out.println(Files.walk(dbPath)
//                .sorted(Comparator.reverseOrder())
//                .mapToLong(p -> {
//                    try {
//                        return Files.size(p);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }).sum());

//        executorService.shutdown();
//        executorService.shutdownNow();
//        executorService.close();
    }

    private static void justRead(DB db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        int interval = 10000;
        var rand = new Random(SEED);
        var randomSeeds = new ArrayList<Long>();
        for (int i = 0; i < totalEntryCount/interval; i++) {
            randomSeeds.add(rand.nextLong());
        }

        try {
            System.out.println("Reading... ");
            //            Scanner scan = new Scanner(System.in);
            //            scan.nextLine();
            var startTime = System.nanoTime();
            var countDown = randomSeeds.size();
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
            var endTime = System.nanoTime();

            var readingTime = endTime - startTime;
            System.out.println("reading time=" + readingTime/1000_000_000.0);
            System.out.println("Number of threads: " + Thread.activeCount());

            SearchStats searchStats = db.getSearchStats();
            for (Map.Entry<Integer, Long> integerLongEntry : searchStats.fileSeekHistogram().entrySet()) {
                System.out.println("numberOfFilesRequiredToSearch=" + integerLongEntry.getKey() +
                        " numberOfTimesThisHappened=" + integerLongEntry.getValue());
            }
            System.out.println("total searches " + searchStats.totalSearches());
            System.out.println("totalValuesFromCache " +searchStats.totalValuesFromCache());
            System.out.println("successfulSearchCount " + searchStats.successfulSearchCount());
            System.out.println("unsuccessfulSearchCount " + searchStats.unsuccessfulSearchCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
        }
    }


    private static void benchmarkWithRandomKVBytesWithNoInMemoryHold(DB db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
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

            SearchStats searchStats = db.getSearchStats();
            for (Map.Entry<Integer, Long> integerLongEntry : searchStats.fileSeekHistogram().entrySet()) {
                System.out.println("numberOfFilesRequiredToSearch=" + integerLongEntry.getKey() +
                        " numberOfTimesThisHappened=" + integerLongEntry.getValue());
            }
            System.out.println("total searches " + searchStats.totalSearches());
            System.out.println("totalValuesFromCache " +searchStats.totalValuesFromCache());
            System.out.println("successfulSearchCount " + searchStats.successfulSearchCount());
            System.out.println("unsuccessfulSearchCount " + searchStats.unsuccessfulSearchCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
        }
    }


    private static void benchmarkWithRandomKVBytes(DB db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
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
            AtomicBoolean isCrashed = new AtomicBoolean(false);
            list.forEach(each -> {
                try {
                    db.get(each);
//                    System.out.println(Arrays.toString(db.get(each)));
                } catch (Exception e) {
                    isCrashed.set(true);
                    e.printStackTrace();
//                    throw new RuntimeException(e);
                } finally {
                    if (isCrashed.get()) {
                        System.out.println("Crashed while reading, exiting...");
                        System.exit(1);
                    }
                }
            });
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised in mb="+(actualMemUsed)/(1024 * 1024));
            System.out.println("Number of threads: " + Thread.activeCount());

            SearchStats searchStats = db.getSearchStats();
            for (Map.Entry<Integer, Long> integerLongEntry : searchStats.fileSeekHistogram().entrySet()) {
                System.out.println("numberOfFilesRequiredToSearch=" + integerLongEntry.getKey() +
                        " numberOfTimesThisHappened=" + integerLongEntry.getValue());
            }
            System.out.println("total searches " + searchStats.totalSearches());
            System.out.println("totalValuesFromCache " +searchStats.totalValuesFromCache());
            System.out.println("successfulSearchCount " + searchStats.successfulSearchCount());
            System.out.println("unsuccessfulSearchCount " + searchStats.unsuccessfulSearchCount());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
        }
    }

    private static void benchmarkWithRandomKVBytesWith3Reads(DB db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
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

            System.out.println("Reading... ");
            var list = new ArrayList<>(map.keySet());
//            Scanner scan = new Scanner(System.in);
//            scan.nextLine();
//            System.out.println("Reading first time in sequential order... ");
//            startTime = System.nanoTime();
//            list.forEach(each -> {
//                try {
//                    db.get(each);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
//            System.out.println((endTime-startTime)/1000_000_000.0 + " for sequential, Reading again in reverse... ");
//            startTime = System.nanoTime();
//            list.reversed().forEach(each -> {
//                try {
//                    db.get(each);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
//            System.out.println((endTime-startTime)/1000_000_000.0 + " for reversed, Reading again in random... ");
//            Collections.shuffle(list);
//            startTime = System.nanoTime();
//            list.forEach(each -> {
//                try {
//                    db.get(each);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            endTime = System.nanoTime();
            System.out.println((endTime-startTime)/1000_000_000.0 + " for shuffled, Reading again in random index... ");
            Random random = new Random(SEED);
            startTime = System.nanoTime();
            for (int j = 0; j < list.size(); j++) {
                db.get(list.get(random.nextInt(10000)));
            }
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised in mb="+(actualMemUsed)/(1024 * 1024));
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
        }
    }

    private static Map<byte[], byte[]> getRandomKV(int totalEntryCount, Supplier<Integer> keyBytesLength, Supplier<Integer> valueBytesLength) {
        // total entries
        System.out.println("random generation");
        long bytesCount = 0;
        var rand = new Random(SEED);
        Map<byte[], byte[]> map = new HashMap<>(totalEntryCount);
        for (int i = 0; i < totalEntryCount; i++) {
            var key = new byte[rand.nextInt(10, keyBytesLength.get())];
            var value = new byte[rand.nextInt(10, valueBytesLength.get())];
            rand.nextBytes(key); rand.nextBytes(value);
            bytesCount += key.length + value.length;
            map.put(key, value);
        }
        // end
        System.out.println("Total bytes generated: " + bytesCount);
        return map;
    }
}

/**
 *
 * writing time=11.8468427 , reading time=17.1859675
 *
 *
 * ### original approach with streams.
 * 10_000_00
 * Reading...
 * writing time=5.1814216 , reading time=8.5731288
 * memory utilised=951983472
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=911574
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=32244
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=607
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=7
 * Number of actually compactions: 28
 * LEVEL_ZERO has 18902134 size in bytes
 * LEVEL_ZERO has 1 files
 * LEVEL_ONE has 89952988 size in bytes
 * LEVEL_ONE has 19 files
 * LEVEL_TWO has 515123690 size in bytes
 * LEVEL_TWO has 109 files
 *
 * 20_000_00
 * writing time=11.6187411 , reading time=23.6619881
 * memory utilised=954069984
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1866389
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=80303
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=2311
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=46
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=2
 * Number of actually compactions: 65
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 85153762 size in bytes
 * LEVEL_ONE has 18 files
 * LEVEL_TWO has 945064164 size in bytes
 * LEVEL_TWO has 200 files
 * LEVEL_THREE has 236084120 size in bytes
 * LEVEL_THREE has 50 files
 *
 * 20_000_00
 * Reading...
 * writing time=11.44035 , reading time=23.0326739
 * memory utilised=1227749424
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1866389
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=80303
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=2311
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=46
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=2
 * Number of actually compactions: 65
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 85153762 size in bytes
 * LEVEL_ONE has 18 files
 * LEVEL_TWO has 945064164 size in bytes
 * LEVEL_TWO has 200 files
 * LEVEL_THREE has 236084120 size in bytes
 * LEVEL_THREE has 50 files
 *
 * ### proper original
 * 20_000_00
 * writing time=11.9340376 , reading time=21.1650295
 * memory utilised=1217244032
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1838227
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=106285
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4264
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=128
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=3
 * Number of actually compactions: 70
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 85095632 size in bytes
 * LEVEL_ONE has 18 files
 * LEVEL_TWO has 940353022 size in bytes
 * LEVEL_TWO has 199 files
 * LEVEL_THREE has 240895610 size in bytes
 * LEVEL_THREE has 51 files
 *
 * IntervalTree
 * 20_000_00
 * Reading...
 * writing time=11.5945271 , reading time=17.9180796
 * memory utilised=1359678568
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1837823
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=107020
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=3899
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=93
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 82
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 80233056 size in bytes
 * LEVEL_ONE has 17 files
 * LEVEL_TWO has 694551794 size in bytes
 * LEVEL_TWO has 147 files
 * LEVEL_THREE has 491108792 size in bytes
 * LEVEL_THREE has 104 files
 *
 * Same
 * Reading...
 * writing time=11.4522867 , reading time=20.8206918
 * memory utilised=1693467432
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1838887
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=106009
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4154
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=118
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=3
 * Number of actually compactions: 73
 * LEVEL_ZERO has 18899140 size in bytes
 * LEVEL_ZERO has 1 files
 * LEVEL_ONE has 132788168 size in bytes
 * LEVEL_ONE has 28 files
 * LEVEL_TWO has 987546670 size in bytes
 * LEVEL_TWO has 209 files
 * LEVEL_THREE has 127541834 size in bytes
 * LEVEL_THREE has 27 files
 *
 * Same
 * Reading...
 * writing time=10.2593469 , reading time=18.623732
 * memory utilised=1592084776
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1838887
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=106009
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4154
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=118
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=3
 * Number of actually compactions: 73
 * LEVEL_ZERO has 18899140 size in bytes
 * LEVEL_ZERO has 1 files
 * LEVEL_ONE has 132788168 size in bytes
 * LEVEL_ONE has 28 files
 * LEVEL_TWO has 987546670 size in bytes
 * LEVEL_TWO has 209 files
 * LEVEL_THREE has 127541834 size in bytes
 * LEVEL_THREE has 27 files
 *
 * IntervalRedBlack
 * 20_000_00
 * Reading...
 * writing time=10.5509469 , reading time=19.1972436
 * memory utilised=1144930096
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1831748
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=112120
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4858
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=171
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 81
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 85175396 size in bytes
 * LEVEL_ONE has 18 files
 * LEVEL_TWO has 921375786 size in bytes
 * LEVEL_TWO has 195 files
 * LEVEL_THREE has 259759662 size in bytes
 * LEVEL_THREE has 55 files
 *
 * Same
 * Reading...
 * writing time=11.507525 , reading time=17.9729444
 * memory utilised=1177067584
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1837823
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=107020
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=3899
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=93
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 82
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 80233056 size in bytes
 * LEVEL_ONE has 17 files
 * LEVEL_TWO has 694551794 size in bytes
 * LEVEL_TWO has 147 files
 * LEVEL_THREE has 491108792 size in bytes
 * LEVEL_THREE has 104 files
 *
 * Same
 * Reading...
 * writing time=10.5009975 , reading time=19.3601175
 * memory utilised=1733938720
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1831748
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=112120
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4858
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=171
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 81
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 85175396 size in bytes
 * LEVEL_ONE has 18 files
 * LEVEL_TWO has 921375786 size in bytes
 * LEVEL_TWO has 195 files
 * LEVEL_THREE has 259759662 size in bytes
 * LEVEL_THREE has 55 files
 *
 * TimeLineIntervalSearch
 * 20_000_00
 * Reading...
 * writing time=10.3955442 , reading time=17.7733977
 * memory utilised=1103121328
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1831748
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=112120
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=4858
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=171
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 81
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 85175396 size in bytes
 * LEVEL_ONE has 18 files
 * LEVEL_TWO has 921375786 size in bytes
 * LEVEL_TWO has 195 files
 * LEVEL_THREE has 259759662 size in bytes
 * LEVEL_THREE has 55 files
 *
 * Same
 * Reading...
 * writing time=12.1341711 , reading time=16.7875514
 * memory utilised=1124377256
 * Number of threads: 12
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1837823
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=107020
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=3899
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=93
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 82
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 80233056 size in bytes
 * LEVEL_ONE has 17 files
 * LEVEL_TWO has 694551794 size in bytes
 * LEVEL_TWO has 147 files
 * LEVEL_THREE has 491108792 size in bytes
 * LEVEL_THREE has 104 files
 *
 * Same
 * Reading...
 * writing time=12.1108324 , reading time=16.9314023
 * memory utilised=1264336992
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1837823
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=107020
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=3899
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=93
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 82
 * LEVEL_ZERO has 0 size in bytes
 * LEVEL_ZERO has 0 files
 * LEVEL_ONE has 80233056 size in bytes
 * LEVEL_ONE has 17 files
 * LEVEL_TWO has 694551794 size in bytes
 * LEVEL_TWO has 147 files
 * LEVEL_THREE has 491108792 size in bytes
 * LEVEL_THREE has 104 files
 *
 *
 * writing time=9.5039227 , reading time=18.7397814
 * memory utilised=1108344352
 * Number of threads: 7
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1861734
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=84646
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=2596
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=78
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 46
 * LEVEL_ZERO has 18900828 size in bytes
 * LEVEL_ZERO has 1 files
 * LEVEL_ONE has 368524978 size in bytes
 * LEVEL_ONE has 78 files
 * LEVEL_TWO has 879136722 size in bytes
 * LEVEL_TWO has 186 files
 *
 * Interval
 * writing time=11.1833474 , reading time=17.7931387 avg
 * memory utilised=2079426944
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1864692
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=82038
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=2279
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=45
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 46
 * LEVEL_ZERO has 18900828 size in bytes
 * LEVEL_ZERO has 1 files
 * LEVEL_ONE has 368524978 size in bytes
 * LEVEL_ONE has 78 files
 * LEVEL_TWO has 879136722 size in bytes
 * LEVEL_TWO has 186 files
 *
 * RedBlack
 * writing time=10.6726335 , reading time=16.344174966666667 avg
 * memory utilised=1689131632
 * Number of threads: 6
 * numberOfFilesRequiredToSearch=0 numberOfTimesThisHappened=81750
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1805195
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=60562
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=1523
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=24
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 * Number of actually compactions: 46
 * LEVEL_ZERO has 18900828 size in bytes
 * LEVEL_ZERO has 1 files
 * LEVEL_ONE has 368524978 size in bytes
 * LEVEL_ONE has 78 files
 * LEVEL_TWO has 879136722 size in bytes
 * LEVEL_TWO has 186 files
 *
 * Timebased
 writing time=10.7055868 , reading time=16.943286099999998
 memory utilised=1156747552
 Number of threads: 6
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1864692
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=82038
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=2279
 numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=45
 numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 Number of actually compactions: 46
 LEVEL_ZERO has 18900828 size in bytes
 LEVEL_ZERO has 1 files
 LEVEL_ONE has 368524978 size in bytes
 LEVEL_ONE has 78 files
 LEVEL_TWO has 879136722 size in bytes
 LEVEL_TWO has 186 files


 InMemory         benchmarkWithRandomKVBytes(db, 20_000_00, 500, 50);
 writing time=12.1739265 , reading time=22.6779272
 memory utilised in mb=1683
 Number of threads: 13
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened= 96.6888453650249%
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened= 3.257174553214765%
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened= 0.053518274977204704%
 numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened= 4.618067831206542E-4%

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
 *
 *
 *
 *
 * Caffiene cache
 * writing time=187.9445439 , reading time=1205.9599506
 * memory utilised In mb=703
 * Number of threads: 17
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=18
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=637
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=20428
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=469131
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=5488959
 * total searches 6000000
 * totalValuesFromCache 20824
 * successfulSearchCount 6000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 1156.7564355 seconds
 * Time taken for file seek: 894.6838945 seconds
 * Time taken for cache read: 47.9231114 seconds
 * Number of actually compactions: 154
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 80 size in bytes
 * LEVEL_ONE has 80 files
 * LEVEL_TWO has 486 size in bytes
 * LEVEL_TWO has 486 files
 * LEVEL_THREE has 993 size in bytes
 * LEVEL_THREE has 993 files
 * LEVEL_FOUR has 0 size in bytes
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 size in bytes
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 size in bytes
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 size in bytes
 * LEVEL_SEVEN has 0 files
 *
 * My cache
 * writing time=129.3346592 , reading time=1295.5181186
 * memory utilised In mb=719
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=18
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=637
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=20428
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=469131
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=5488959
 * total searches 6000000
 * totalValuesFromCache 20824
 * successfulSearchCount 6000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 1245.5284196 seconds
 * Time taken for file seek: 658.8651988 seconds
 * Time taken for cache read: 397.1020263 seconds
 * Number of actually compactions: 154
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 80 size in bytes
 * LEVEL_ONE has 80 files
 * LEVEL_TWO has 486 size in bytes
 * LEVEL_TWO has 486 files
 * LEVEL_THREE has 993 size in bytes
 * LEVEL_THREE has 993 files
 *
 *
 * 10000000 AtomDB with my cache.
 * writing time=305.908781 , reading time=705.1794479
 * memory utilised In mb=1135
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=8 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=37
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=753
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=8999
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=73901
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=434653
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=2193563
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=7263554
 * total searches 10000000
 * totalValuesFromCache 24538
 * successfulSearchCount 10000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 607.4117132 seconds
 * Time taken for file seek: 444.8294423 seconds
 * Time taken for cache read: 4.1673642 seconds
 * Number of actually compactions: 273
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 11 size in bytes
 * LEVEL_ONE has 11 files
 * LEVEL_TWO has 88 size in bytes
 * LEVEL_TWO has 88 files
 * LEVEL_THREE has 373 size in bytes
 * LEVEL_THREE has 373 files
 * LEVEL_FOUR has 0 size in bytes
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 size in bytes
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 size in bytes
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 size in bytes
 * LEVEL_SEVEN has 0 files
 *
 * 10000000
 * writing time=2580.6015976 , reading time=5958.2694991
 * memory utilised In mb=1136
 * Number of threads: 3
 *
 * 10000000 atomdb in normal condition
 * memory utilised In mb=1291
 * numberOfFilesRequiredToSearch=8 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=753
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=73901
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=2193563
 * total searches 10000000
 * successfulSearchCount 10000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 617.2256864 seconds
 * Time taken for file seek: 458.2941976 seconds
 * Time taken for cache read: 4.4560553 seconds
 * Number of actually compactions: 273
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 11 size in bytes
 * LEVEL_ONE has 11 files
 * LEVEL_TWO has 88 size in bytes
 * LEVEL_TWO has 88 files
 * LEVEL_THREE has 373 size in bytes
 * LEVEL_THREE has 373 files
 *
 * writing time=94.3120473 , reading time=108.768555
 * memory utilised In mb=1309
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=3675307
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=291134
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=14034
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=532
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=24
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=1
 * total searches 4000000
 * totalValuesFromCache 18967
 * successfulSearchCount 4000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 88.5151143 seconds
 * Time taken for file seek: 72.7241652 seconds
 * Time taken for cache read: 0.7457204 seconds
 * Number of actually compactions: 127
 * LEVEL_ZERO has 2 size in bytes
 * LEVEL_ZERO has 2 files
 * LEVEL_ONE has 22 size in bytes
 * LEVEL_ONE has 22 files
 * LEVEL_TWO has 82 size in bytes
 * LEVEL_TWO has 82 files
 * LEVEL_THREE has 107 size in bytes
 * LEVEL_THREE has 107 files
 *
 * AtomDB 10000000 with random on compaction for partial compaction and full compaction.
 * writing time=258.5519731 , reading time=449.7186792
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=43
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=903
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=14999
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=184115
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=1617135
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=8158266
 * total searches 10000000
 * totalValuesFromCache 24538
 * successfulSearchCount 10000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 364.7084685 seconds
 * Time taken for file seek: 239.3307144 seconds
 * Time taken for cache read: 3.4588717 seconds
 * Number of actually compactions: 304
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 15 size in bytes
 * LEVEL_ONE has 15 files
 * LEVEL_TWO has 104 size in bytes
 * LEVEL_TWO has 104 files
 * LEVEL_THREE has 359 size in bytes
 * LEVEL_THREE has 359 files
 * LEVEL_FOUR has 0 size in bytes
 * LEVEL_FOUR has 0 files
 * LEVEL_FIVE has 0 size in bytes
 * LEVEL_FIVE has 0 files
 * LEVEL_SIX has 0 size in bytes
 * LEVEL_SIX has 0 files
 * LEVEL_SEVEN has 0 size in bytes
 * LEVEL_SEVEN has 0 files
 *
 * AtomDB 10000000 with random on compaction for partial compaction and full compaction. (level sepcific alternator)
 * writing time=246.8832036 , reading time=494.7827544
 * memory utilised In mb=1421
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=8
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=188
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=5477
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=106964
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=1344283
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=8518541
 * total searches 10000000
 * totalValuesFromCache 24538
 * successfulSearchCount 10000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 409.7651074 seconds
 * Time taken for file seek: 301.1809406 seconds
 * Time taken for cache read: 3.1556527 seconds
 * Number of actually compactions: 304
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 7 size in bytes
 * LEVEL_ONE has 7 files
 * LEVEL_TWO has 63 size in bytes
 * LEVEL_TWO has 63 files
 * LEVEL_THREE has 392 size in bytes
 * LEVEL_THREE has 392 files
 *
 *         benchmarkWithRandomKVBytesWithNoInMemoryHold(db, 20_000_00, 500, 50); with filechannel and same tech as above result
 * writing time=105.0264001 , reading time=100.6091279
 * memory utilised In mb=465
 * Number of threads: 9
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=21407
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=1241
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=45
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=1
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=1712801
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=247395
 * total searches 2000000
 * totalValuesFromCache 17110
 * successfulSearchCount 2000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 89.327686 seconds
 * Time taken for file seek: 82.7209674 seconds
 * Time taken for cache read: 0.2923511 seconds
 * Number of actually compactions: 52
 * LEVEL_ZERO has 3 size in bytes
 * LEVEL_ZERO has 3 files
 * LEVEL_ONE has 15 size in bytes
 * LEVEL_ONE has 15 files
 * LEVEL_TWO has 110 size in bytes
 * LEVEL_TWO has 110 files
 *
 * 10000000 after level enum class change
 * writing time=295.6735699 , reading time=452.2195548
 * memory utilised In mb=1537
 * Number of threads: 10
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=8146304
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=1633207
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=181727
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=13488
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=709
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=26
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * total searches 10000000
 * totalValuesFromCache 24538
 * successfulSearchCount 10000000
 * unsuccessfulSearchCount 0
 * Overall time taken for file seek: 352.9782128 seconds
 * Time taken for file seek: 206.9478356 seconds
 * Time taken for cache read: 3.9635908 seconds
 * Number of actually compactions: 298
 * org.g2n.atomdb.level.Level@0 has 3 size in bytes
 * org.g2n.atomdb.level.Level@0 has 3 files
 * org.g2n.atomdb.level.Level@1 has 10 size in bytes
 * org.g2n.atomdb.level.Level@1 has 10 files
 * org.g2n.atomdb.level.Level@2 has 111 size in bytes
 * org.g2n.atomdb.level.Level@2 has 111 files
 * org.g2n.atomdb.level.Level@3 has 355 size in bytes
 * org.g2n.atomdb.level.Level@3 has 355 files
 * Is number of files less than the limit for true
 * Is number of files less than the limit for true
 * Is number of files less than the limit for true
 * Is number of files less than the limit for true
 * We are done here/////////////////////////
 * 0
 */
