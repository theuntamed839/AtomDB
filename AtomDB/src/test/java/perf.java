import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class perf {

    public static final int SEED = 1234;

    public static void main(String[] args) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();

        System.out.println("Total Memory: " + totalMemory / (1024 * 1024) + " MB");
        System.out.println("Free Memory: " + freeMemory / (1024 * 1024) + " MB");
        System.out.println("Max Memory: " + maxMemory / (1024 * 1024) + " MB");
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

        var opt = new DbOptions();
        opt.disallowUseOfMMap();
        var jimfs = Jimfs.newFileSystem(Configuration.unix());
        var dbPath = Files.createTempDirectory(jimfs.getPath("/"), "benchmarkWithRandomKVBytes_" + getSaltString());
        var db = new DBImpl(dbPath, opt);

        benchmarkWithRandomKVBytes(db, 20_000_00, 500, 50);
        System.out.println(Files.size(dbPath));
        System.out.println(Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).sum());
        executorService.shutdown();
        executorService.shutdownNow();
        executorService.close();
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
        }
    }

    static String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random(SEED);
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

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
 */
