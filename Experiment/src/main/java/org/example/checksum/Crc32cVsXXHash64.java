package org.example.checksum;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.util.*;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

public class Crc32cVsXXHash64 {
    public static void main(String[] args) {
        benchMark(1024, 100);
        benchMark(1024, 1000);
        benchMark(1024, 10000);
        benchMark(1024, 100000);
        benchMark(1024, 1000000);
        benchMark(1024, 10000000);
    }
    public static void benchMark(int byteArraySize, int numberOfIterations) {
        System.out.println("BENCHMARK for bitSize="+byteArraySize + " iter=" + numberOfIterations);
        Random rand = new Random();
        List<byte[]> entries = new ArrayList<>(numberOfIterations);
        for (int i = 0; i < numberOfIterations; i++) {
            byte[] array = new byte[byteArraySize];
            rand.nextBytes(array);
            entries.add(array);
        }
        xxhash64(entries);
        crc32cJava(entries);
        crc32cSeperateClass(entries);
        crc32cGuava(entries);
    }

    public static void crc32cJava(List<byte[]> entries) {
        var crc32C = new CRC32C();
        long start, end;
        start = System.nanoTime();
        for (byte[] entry : entries) {
            crc32C.update(entry);
            crc32C.getValue();
            crc32C.reset();
        }
        end = System.nanoTime();
        System.out.println("crc32c java took="+(end - start));
    }

    public static void xxhash64(List<byte[]> entries) {
        var xxHash64 = XXHashFactory.fastestInstance().hash64();
        long start, end;
        start = System.nanoTime();
        for (byte[] entry : entries) {
            xxHash64.hash(entry, 0, entry.length, 0x9747b28cL);
        }
        end = System.nanoTime();
        System.out.println("xxHash64 java took="+(end - start));
    }

    public static void crc32cSeperateClass(List<byte[]> entries) {
        var crc32C = new PureJavaCrc32C();
        long start, end;
        start = System.nanoTime();
        for (byte[] entry : entries) {
            crc32C.update(entry);
            crc32C.getValue();
            crc32C.reset();
        }
        end = System.nanoTime();
        System.out.println("crc32cSeperateClass java took="+(end - start));
    }

    public static void crc32cGuava(List<byte[]> entries) {
        HashFunction crc32C = Hashing.crc32c();
        long start, end;
        start = System.nanoTime();
        for (byte[] entry : entries) {
            var hashCode = crc32C.hashBytes(entry);
            hashCode.asInt();
        }
        end = System.nanoTime();
        System.out.println("guava java took="+(end - start));
    }
}
