package org.example;

import com.sahilbondre.firefly.FireflyDB;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class FireFlyDBBenchmark {

    public static void main(String[] args) throws Exception {
        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(5);
        System.out.println("Warm Up with 50k");
        benchmark(inputString, 50000, "12");
        benchmark(inputString, 1000, "213");
        benchmark(inputString, 10000, "12312");
        benchmark(inputString, 100000, "asdasdas");
        benchmark(inputString, 1000000, "asd");
    }

    public static void benchmark(String inputString, long totalEntryCount, String dbName) throws Exception {
        Files.createDirectories(Paths.get(dbName + "_fireflyDB"));
        FireflyDB db =FireflyDB.getInstance(dbName + "_fireflyDB");
        db.start();
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            for (int i = 0; i < totalEntryCount; i++) {
                db.set(bytes(i + ""), bytes(i + "_" + inputString));
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.stop();
        }
    }
}
