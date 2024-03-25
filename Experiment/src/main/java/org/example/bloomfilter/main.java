package org.example.bloomfilter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32C;

public class main {
    public static void main(String[] args) throws IOException {
        bloomIntegerBenchMark();
        bloomBenchMark();
        Crc32cBenchMark();
    }
    public static void Crc32cBenchMark() {
        Random rand = new Random();
        var crc32c = new CRC32C();
        List<Integer> list = new ArrayList<>();
        List<byte[]> byteHolder = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] array = new byte[1024];
            rand.nextBytes(array);
            crc32c.update(array);
            list.add((int) crc32c.getValue());
            byteHolder.add(array);
            crc32c.reset();
        }

        for (byte[] bytes : byteHolder) {
            crc32c.update(bytes);
            if (list.contains((int)crc32c.getValue())) {
                System.out.println("check with existing one");
            }
            crc32c.reset();
        }

        long start, end;
        start = System.nanoTime();
        byte[] array = new byte[1024];
        int gotCount = 0;
        for (int i = 0; i < 100_000_000; i++) {
            rand.nextBytes(array);
            crc32c.update(array);
            if (list.contains((int)crc32c.getValue())) {
                gotCount++;
            }
            crc32c.reset();
        }
        end = System.nanoTime();
        System.out.println("got="+gotCount);
        System.out.println("took"+(end-start));
    }

    public static void bloomBenchMark() throws IOException {
        var myWriter = new writer();
        BloomFilter<byte[]> filter = BloomFilter.create(
                Funnels.byteArrayFunnel(),
                10,
                0.00001);
        Random rand = new Random();
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] array = new byte[1024];
            rand.nextBytes(array);
            list.add(array);
            filter.put(array);
        }
        filter.writeTo(myWriter);
        myWriter.close();

        long start, end;
        start = System.nanoTime();

        byte[] array = new byte[1024];
        int gotCount = 0;
        for (int i = 0; i < 100_000_000; i++) {
            rand.nextBytes(array);
            if (filter.mightContain(array)) {
//                for (byte[] bytes : list) {
//                    if (Arrays.compare(bytes, array) == 0) {
//                        gotCount--;
//                        System.out.println("was there");
//                    }
//                }
                gotCount++;
            }
        }
        end = System.nanoTime();
        System.out.println("got="+gotCount);
        System.out.println("took"+(end-start));
    }

    public static void bloomIntegerBenchMark() throws IOException {
        var myWriter = new writer();
        BloomFilter<Integer> filter = BloomFilter.create(
                Funnels.integerFunnel(),
                10,
                0.0000000001);
        Random rand = new Random();
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int i1 = rand.nextInt();
            list.add(i1);
            filter.put(i1);
        }
        filter.writeTo(myWriter);
        myWriter.close();

        long start, end;
        start = System.nanoTime();

        int gotCount = 0;
        for (int i = 0; i < 100_000_000; i++) {
            int i1 = rand.nextInt();
            if (filter.mightContain(i1)) {
                if (list.contains(i1)) {
                    gotCount--;
                }
                gotCount++;
            }
        }
        end = System.nanoTime();
        System.out.println("got="+gotCount);
        System.out.println("took"+(end-start));
    }
}

class writer extends OutputStream implements AutoCloseable {
    int count = 0;
    @Override
    public void write(int b) {
        count++;
    }

    @Override
    public void close() throws IOException {
        super.close();
        System.out.println("count="+count);
    }
}
/**
 * count=126 byte required
 * 9139 false, for 100_000_000
 *
 */
