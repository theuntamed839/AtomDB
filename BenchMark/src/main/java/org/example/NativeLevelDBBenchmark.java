package org.example;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class NativeLevelDBBenchmark {
    public static void main(String[] args) throws Exception {
        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(1000000);
        System.out.println("Warm Up with 50k");
        benchmark(inputString, 50000, "12");
        benchmark(inputString, 1000, "213");
        benchmark(inputString, 10000, "12312");
        benchmark(inputString, 100000, "asdasdas");
        benchmark(inputString, 1000000, "asd");
        System.out.println("random read");
        benchmarkRandomRead(inputString, 1000000, "asd");
    }

    private static void benchmarkRandomRead(String inputString, long totalEntryCount, String dbName) throws IOException {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        Options options = new Options();
        options.createIfMissing(true);
        DB db = factory.open(new File(dbName + "_db"), options);
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

    public static void benchmark(String inputString, long totalEntryCount, String dbName) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        Options options = new Options();
        options.createIfMissing(true);
        DB db = factory.open(new File(dbName + "_db"), options);
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
            Files.walk(Path.of(dbName + "_db"))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}

// 1000000 disabled cache write 22 sec and read 3