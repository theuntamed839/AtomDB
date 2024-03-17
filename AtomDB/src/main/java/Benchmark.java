import db.DB;
import db.DBImpl;
import db.DBOptions;
import org.xerial.snappy.Snappy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

import static util.BytesConverter.bytes;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(5);
        System.out.println("Warm Up with 50k");
        //benchmark(inputString, 500000);
//        benchmark(inputString, 1000);
//        benchmark(inputString, 10000);
//        benchmark(inputString, 100000);
        benchmark(inputString, 1000000);
//        benchmarkWriting(inputString, 1000000);
//        initialTest(inputString, 50000);
//        benchmarkRandomRead(inputString, 1000000, "asd");
    }

    private static void benchmarkRandomRead(String inputString, long totalEntryCount, String dbName) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        var opt = new DBOptions();
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
            Files.walk(Path.of(dbName + "_db"))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    public static void initialTest(String inputString, long totalEntryCount) throws Exception {
        var opt = new DBOptions();
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
        var opt = new DBOptions();
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
        var opt = new DBOptions();
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
            db.destroy();
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

// optimize branch write=8.7 secs and read=2mins, all files in level0 and sparse 0.80
