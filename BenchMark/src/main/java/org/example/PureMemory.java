package org.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class PureMemory {
    public static void main(String[] args) throws Exception {
        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(5);
        benchmark(inputString, 1000000, "asd");
        System.out.println("random read");
        benchmarkRandomRead(inputString, 1000000, "asd");
    }

    private static void benchmarkRandomRead(String inputString, long totalEntryCount, String dbName) throws IOException {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long startTime , endTime, readingTime, writingTime;
        Map<byte[], byte[]> db = new HashMap<>();
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
        }
    }

    public static void benchmark(String inputString, long totalEntryCount, String dbName) throws Exception {
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long startTime , endTime, readingTime, writingTime;
        Map<byte[], byte[]> db = new HashMap<>();
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
        }
    }
}