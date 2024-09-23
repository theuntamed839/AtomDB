package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main2 {
    public static void main(String[] args) throws IOException {
        int numberOfEntries = 50000;
        List<byte[]> items = new ArrayList<>();
        var random = new Random();
        for (int i = 0; i < numberOfEntries; i++) {
            var temp = new byte[500];
            random.nextBytes(temp);
            items.add(temp);
        }
        items.sort(Arrays::compare);

        BloomFilter<byte[]> filter = BloomFilter.create(
                Funnels.byteArrayFunnel(),
                numberOfEntries,
                0.01);

        items.forEach(filter::put); // insertion in filter.
        FileOutputStream out = new FileOutputStream("idk.txt");
        filter.writeTo(out);
        out.close();
        FileInputStream in = new FileInputStream("idk.txt");

        MappedByteBuffer map = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, in.getChannel().size());
        InputStream inputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                System.out.println(map.remaining());
                return map.get();
            }
        };


        BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(inputStream, Funnels.byteArrayFunnel());
        for (byte[] item : items) {
            if (!bloomFilter.mightContain(item)) {
                System.out.println("faulty bloom");
            }
        }
    }
}
