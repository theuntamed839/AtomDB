package org.example.checksum;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.xerial.snappy.PureJavaCrc32C;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32C;

public class compare {
    public static void main(String[] args) {
        List<BenchMark> benchMarks = List.of(new JavaCrc32CBuiltIn(),
                new GuavaCrc32c(), new hadoopCrc32c(), new SnappyCrc32c());
        benchMark(1024, 10, benchMarks);
        benchMark(1024, 100, benchMarks);
        benchMark(1024, 1000, benchMarks);
        benchMark(1024, 10000, benchMarks);
        benchMark(1024, 100000, benchMarks);
        benchMark(1024, 1000000, benchMarks);
        benchMark(256, 10000000, benchMarks);
    }

    public static void benchMark(int byteArraySize, int numberOfIterations, List<BenchMark> benchMarkList) {
        System.out.println("BENCHMARK for bitSize="+byteArraySize + " iter=" + numberOfIterations);
        var crc32C = new org.example.checksum.PureJavaCrc32C();
        Map<byte[], Long> values = new HashMap<>(numberOfIterations);
        Random rand = new Random();
        for (int i = 0; i < numberOfIterations; i++) {
            byte[] array = new byte[byteArraySize];
            rand.nextBytes(array);
            crc32C.update(array);
            values.put(array, crc32C.getValue());
            crc32C.reset();
        }
        for (BenchMark benchMark : benchMarkList) {
            benchMark.benchMark(values);
        }
    }

    interface BenchMark {
        void benchMark(Map<byte[], Long> values);
    }

    public static class JavaCrc32CBuiltIn implements BenchMark {
        CRC32C crc32C = new CRC32C();
        public void benchMark(Map<byte[], Long> values) {
            long start, end;
            start = System.nanoTime();
            int i = 0;
            for (Map.Entry<byte[], Long> entry : values.entrySet()) {
                byte[] key = entry.getKey();
                Long checksum = entry.getValue();
                crc32C.update(key);
                assert crc32C.getValue() == checksum;
                crc32C.reset();
            }
            end = System.nanoTime();
            System.out.println(this.getClass().getName() + " took="+(end - start));
        }
    }

    public static class GuavaCrc32c implements BenchMark {
        HashFunction crc32C = Hashing.crc32c();
        public void benchMark(Map<byte[], Long> values) {
            long start, end;
            start = System.nanoTime();
            int i = 0;
            for (Map.Entry<byte[], Long> entry : values.entrySet()) {
                byte[] key = entry.getKey();
                Long checksum = entry.getValue();
                HashCode hashCode = crc32C.hashBytes(key);
                assert hashCode.asLong() == checksum;
            }
            end = System.nanoTime();
            System.out.println(this.getClass().getName() + " took="+(end - start));
        }
    }

    public static class hadoopCrc32c implements BenchMark {
        org.example.checksum.PureJavaCrc32C crc32C = new org.example.checksum.PureJavaCrc32C();
        public void benchMark(Map<byte[], Long> values) {
            long start, end;
            start = System.nanoTime();
            int i = 0;
            for (Map.Entry<byte[], Long> entry : values.entrySet()) {
                byte[] key = entry.getKey();
                Long checksum = entry.getValue();
                crc32C.update(key);
                assert crc32C.getValue() == checksum;
                crc32C.reset();
            }
            end = System.nanoTime();
            System.out.println(this.getClass().getName() + " took="+(end - start));
        }
    }

    public static class SnappyCrc32c implements BenchMark {
        PureJavaCrc32C crc32C = new PureJavaCrc32C();
        public void benchMark(Map<byte[], Long> values) {
            long start, end;
            start = System.nanoTime();
            int i = 0;
            for (Map.Entry<byte[], Long> entry : values.entrySet()) {
                byte[] key = entry.getKey();
                Long checksum = entry.getValue();
                crc32C.update(key);
                assert crc32C.getValue() == checksum;
                crc32C.reset();
            }
            end = System.nanoTime();
            System.out.println(this.getClass().getName() + " took="+(end - start));
            System.out.println();
        }
    }
}
