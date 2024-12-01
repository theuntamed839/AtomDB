import db.DBImpl;
import db.DbOptions;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static util.BytesConverter.bytes;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(5);
        System.out.println("Warm Up with 50k");
        //searchBenchMark(500000, "benchmarkWithRandomKVBytesWithCompaction");
        //searchBenchMark(500000, "benchmarkWithRandomKVBytesWithoutCompaction");
        //searchBenchMark(500000, "IssueDB");
//         benchmark(inputString, 1000000);
//        benchmark(inputString, 1000);
//        benchmark(inputString, 10000);
//        benchmark(inputString, 100000);
//        benchmark(inputString, 1000_000);
//        benchmarkWriting(inputString, 1000_000);
//        initialTest(inputString, 50000);
//                benchmark(inputString, 15000);
//        benchmarkWithRandomKVBytes(1000000, 50, 500); //500000
        var map = readOrCreateRandomKV(1000000, 50, 500, "KEY_VALUE_LENGTH_FIXED.trash");
        benchmarkWithRandomKVBytes(map);
//        benchmarkWithRandomLengthKVBytes(1000_000);
//        benchmarkRandomRead(inputString, 1000_000, "asd"); //1000000
    }

    private static Map<byte[], byte[]> readOrCreateRandomKV(int iterationCount, int keySize, int valueSize, String fileName) throws IOException, ClassNotFoundException {
        File file = new File(iterationCount + "_" + keySize + "_" + valueSize + "_" + fileName);
        Map<byte[], byte[]> randomKV;
        if (!file.exists()) {
            randomKV = getRandomKV(iterationCount, () -> keySize, () -> valueSize);
            FileOutputStream fos =new FileOutputStream(file);
            ObjectOutputStream oos =new ObjectOutputStream(fos);
            oos.writeObject(randomKV);
            oos.flush();
            oos.close();
            fos.close();
        }else {
            System.out.println("reading KV from system");
            FileInputStream fis=new FileInputStream(file);
            ObjectInputStream ois=new ObjectInputStream(fis);
            randomKV = (HashMap<byte[], byte[]>)ois.readObject();
            ois.close();
            fis.close();
        }
        return randomKV;
    }

    private static void benchmarkWithRandomLengthKVBytes(int totalEntryCount) throws Exception {
        var dbName = "benchmarkWithRandomKVBytes";
        var rand = new Random();
        var map = getRandomKV(totalEntryCount, () -> rand.nextInt(50, 100), () -> rand.nextInt(50, 1000));

        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(dbName), opt);
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            map.entrySet().forEach(each -> {
                try {
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
            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            Files.walk(Path.of(dbName ))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    private static void benchmarkWithRandomKVBytes(int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        var dbName = "benchmarkWithRandomKVBytes";
        var map = getRandomKV(totalEntryCount, () -> keyBytesLength, () -> valueBytesLength);

        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(dbName), opt);
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
            db.close();
            Files.walk(Path.of(dbName ))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    private static void benchmarkWithRandomKVBytes(Map<byte[], byte[]> map) throws Exception {
        var dbName = "benchmarkWithRandomKVBytes";

        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(dbName), opt);
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + map.size());
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
            db.close();
            Files.walk(Path.of(dbName ))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
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

    private static void benchmarkRandomRead(String inputString, long totalEntryCount, String dbName) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(dbName), opt);
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            for (int i = 0; i < totalEntryCount; i++) {
                db.put(bytes(i + ""), bytes(i + "_" + inputString));
            }
            endTime = System.nanoTime();

            writingTime = endTime - startTime;
            System.out.println("Reading... ");
            startTime = System.nanoTime();
            List<Integer> integers = new ArrayList<>(IntStream.range(0, (int) totalEntryCount).boxed().toList());
            Collections.shuffle(integers);
            Collections.shuffle(integers);
            for (int i : integers) {
                db.get(bytes(i + ""));
            }
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            Files.walk(Path.of(dbName ))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    public static void initialTest(String inputString, long totalEntryCount) throws Exception {
        var opt = new DbOptions();
        var db = new DBImpl(new File(Benchmark.class.getName() + "DB"), opt);
        var key = "somegood things";
        System.out.println("compressed keysize around ="+Snappy.compress(bytes(key)).length);
        System.out.println("compressed valueSize ="+Snappy.compress(bytes(inputString)).length);

        try {
            for (int i = 0; i < totalEntryCount; i++) {
                if (i % 17 == 0 ) {
                    db.put(bytes(key + i), bytes(inputString));
                    continue;
                }
                db.put(bytes(i + ""), bytes(i + "_" + inputString));
            }
            for (int i = 0; i < totalEntryCount; i++) {
                if (i % 17 == 0 ) {
                    byte[] bytes = db.get(bytes(key + i));
                    byte[] bytes1 = bytes(inputString);
                    if (Arrays.compare(bytes, bytes1) != 0) {
                        System.out.println("moye moye");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            db.destroy();
        }
    }

    public static void benchmarkWriting(String inputString, long totalEntryCount) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(Benchmark.class.getName() + "DB"), opt);
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            for (int i = 0; i < totalEntryCount; i++) {
                db.put(bytes(i + ""), bytes(i + "_" + inputString));
            }
            endTime = System.nanoTime();

            writingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            db.destroy();
        }
    }

    public static void benchmark(String inputString, long totalEntryCount) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(Benchmark.class.getName() + "DB"), opt);
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            for (int i = 0; i < totalEntryCount; i++) {
                db.put(bytes(i + ""), bytes(i + "_" + inputString));
            }
            endTime = System.nanoTime();

            writingTime = endTime - startTime;
            System.out.println("Writing ="+ writingTime);
            System.out.println("Reading... ");
            startTime = System.nanoTime();
            for (int i = 0; i < totalEntryCount; i++) {
                db.get(bytes(i + ""));
            }
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            //db.destroy();
        }
    }

    public static void searchBenchMark(long totalEntryCount, String DBName) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DbOptions();
        var db = new DBImpl(new File(DBName), opt);
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Reading... ");
            startTime = System.nanoTime();
            for (int i = 0; i < totalEntryCount; i++) {
                db.get(bytes(i + ""));
            }
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("reading time=" + (readingTime/1000_000_000.0));
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
            //db.destroy();
        }
    }
}

// firefly
//Warm Up with 50k
//Writing... 50000
//Reading...
//writing time=1199177900 , reading time=1393644400
//Writing... 1000
//Reading...
//writing time=20145900 , reading time=35852900
//Writing... 10000
//Reading...
//writing time=256371600 , reading time=234508600
//Writing... 100000
//Reading...
//writing time=1272501300 , reading time=2226460200
//Writing... 1000000
//Reading...
//writing time=15237828200 , reading time=22578177500
// 15 and 22

//leveldb
//Warm Up with 50k
//Writing... 50000
//Reading...
//writing time=477759600 , reading time=152400700
//Writing... 1000
//Reading...
//writing time=9359800 , reading time=1390100
//Writing... 10000
//Reading...
//writing time=79710100 , reading time=14241500
//Writing... 100000
//Reading...
//writing time=780837900 , reading time=276792700
//Writing... 1000000
//Reading...
//writing time=10130438600 , reading time=2697346200
// 10 sec and 2.69 sec

//atomDB
//Warm Up with 50k
//Writing... 50000
//Reading...
//writing time=961983000 , reading time=7546111700
//Writing... 1000
//Reading...
//writing time=4872700 , reading time=414700
//Writing... 10000
//Reading...
//writing time=44763700 , reading time=4071100
//Writing... 10000_0
//Reading...
//writing time=3879929800 , reading time=18500214900
//Writing... 10000_00
//Reading...
//writing time=95125721700 , reading time=204819288000
//95 sec and 3.4 minutes

//writing time=75009226700 , reading time=159444240000
// 75 sec and 2.65 minutes with snappy

// levelDB writing 10.13 sec and 2.69 sec reading
// 15 and 22 firefly

//writing time=93463754000 , reading time=167366800200 without snappy
// 93 sec and 167
//writing time=54990968100 , reading time=162650142200 with snappy
//54 sec and 162 secs

// levelDB writing 10.13 sec and 2.69 sec reading
// 15 and 22 firefly

/**
 *  optimized Champion, without compaction and 0.8 sparse binary search
 *  basically we have 2^(log(n) * 0.8) keys in memory.
 *  writing ~5sec for all cases and reading ~2sec and for random reading ~23
 *  need to work on random searches as this is the real world scenario.
 *     Warm Up with 50k
 *         Number of threads: 2
 *         Writing... 500000
 *         Writing =3381345200
 *         Reading...
 *         writing time=3381345200 , reading time=1014120400
 *         memory utilised=181201096
 *         Number of threads: 2
 *         Number of threads: 2
 *         Writing... 1000
 *         Writing =19922100
 *         Reading...
 *         writing time=19922100 , reading time=751600
 *         memory utilised=8125712
 *         Number of threads: 2
 *         Number of threads: 2
 *         Writing... 10000
 *         Writing =48545900
 *         Reading...
 *         writing time=48545900 , reading time=7901400
 *         memory utilised=25168168
 *         Number of threads: 2
 *         Number of threads: 2
 *         Writing... 100000
 *         Writing =776112400
 *         Reading...
 *         writing time=776112400 , reading time=133118300
 *         memory utilised=36181552
 *         Number of threads: 2
 *         Number of threads: 2
 *         Writing... 1000000
 *         Writing =5863752200
 *         Reading...
 *         writing time=5863752200 , reading time=2089110800
 *         memory utilised=415614712
 *         Number of threads: 2
 *         Number of threads: 2
 *         Writing... 1000000
 *         Reading...
 *         writing time=5419208300 , reading time=23297950100
 *         memory utilised=196422816
 *         Number of threads: 2
 */


/**
 * Optimized branch, with full key -> pointer in memory. read is less than 2sec and for less than 20 sec for random
 * Warm Up with 50k
 * Number of threads: 2
 * Writing... 500000
 * Writing =3738520700
 * Reading...
 * writing time=3738520700 , reading time=856879800
 * memory utilised=270531856
 * Number of threads: 2
 * Number of threads: 2
 * Writing... 1000
 * Writing =5127700
 * Reading...
 * writing time=5127700 , reading time=705500
 * memory utilised=7937344
 * Number of threads: 2
 * Number of threads: 2
 * Writing... 10000
 * Writing =48499300
 * Reading...
 * writing time=48499300 , reading time=7948100
 * memory utilised=25168168
 * Number of threads: 2
 * Number of threads: 2
 * Writing... 100000
 * Writing =604777900
 * Reading...
 * writing time=604777900 , reading time=152224100
 * memory utilised=106527624
 * Number of threads: 2
 * Number of threads: 2
 * Writing... 1000000
 * Writing =5834791600
 * Reading...
 * writing time=5834791600 , reading time=1988543400
 * memory utilised=265655992
 * Number of threads: 2
 * Number of threads: 2
 * Writing... 1000000
 * Reading...
 * writing time=5660492600 , reading time=19979041500
 * memory utilised=321943400
 * Number of threads: 2
 */


/**
 * Important
 * benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB_NATIVE),1000000, 50, 500);
 * writing time=93.0521691 , reading time=16.5716184
 * memory utilised=708003848
 * Number of threads: 4
 *
 * benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB),1000000, 50, 500);
 * memory utilised=676719848
 * writing time=75.4093294 , reading time=22.438489
 * memory utilised=676719848
 * Number of threads: 2
 *
 * benchmarkWithRandomKVBytes(DBProvider.get(DB.FIREFLYDB),1000000, 50, 500);
 * writing time=14.2588117 , reading time=32.790273
 * memory utilised=496074880
 * Number of threads: 2
 *
 * ATOMDB
 * writing time=17.8601132 , reading time=22.0283714
 * memory utilised=889973640
 * Number of threads: 7
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=985518
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=9936
 *
 writing time=25.6880718 , reading time=6.0737237
 memory utilised=634179168
 Number of threads: 10
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=966622
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=28525
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=303
 numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=4

 writing time=22.1107311 , reading time=6.3042568
 memory utilised=311031168
 Number of threads: 8
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=975473
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=19890
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=91

 writing time=16.721739 , reading time=6.9919566 (last)
 memory utilised=777919944
 Number of threads: 9
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=956064
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=38839
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=543
 numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=8

 writing time=19.1802351 , reading time=6.2431539 (Executors service)
 memory utilised=182085760
 Number of threads: 9
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=975473
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=19890
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=91

 writing time=19.640476 , reading time=5.8344966(Inline completableFuture)
 memory utilised=631710240
 Number of threads: 9
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=975473
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=19890
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=91

 writing time=17.7529537 , reading time=6.2738548
 memory utilised=586745584
 Number of threads: 7
 numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=956467
 numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=38370
 numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=608
 numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=8
 numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=1
 */