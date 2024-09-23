package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        int numberOfEntries = 50000;
        List<byte[]> items = new ArrayList<>();
        var random = new Random();
        for (int i = 0; i < numberOfEntries; i++) {
            var temp = new byte[50];
            random.nextBytes(temp);
            items.add(temp);
        }
        items.sort(Arrays::compare);

        BloomFilter<byte[]> filter = BloomFilter.create(
                Funnels.byteArrayFunnel(),
                numberOfEntries,
                0.01);

        items.forEach(filter::put); // insertion in filter.

        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        OutputStream out = new OutputStream() {
            @Override
            public void write(byte[] b) throws IOException {
                buffer.put(b);
            }

//            @Override
//            public void write(byte[] b, int off, int len) throws IOException {
//                buffer.put(b, off, len);
//            }

            @Override
            public void write(int i) {
                buffer.put((byte) i);
            }
        };
        filter.writeTo(out);
        buffer.flip();
        InputStream in = new InputStream() {
            @Override
            public int read() {
                return buffer.get();
            }

            @Override
            public int read(byte[] b) throws IOException {
                int position = buffer.position();
                buffer.get(b);
                return buffer.position() - position;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int position = buffer.position();
                buffer.get(b, off, len);
                return buffer.position() - position;
            }
        };

        BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(in, Funnels.byteArrayFunnel());
        for (byte[] item : items) {
            if (!bloomFilter.mightContain(item)) {
                System.out.println("faulty bloom");
            }
        }
    }

    public static List<KVUnit> getListFilledWithKVs(int numberOfEntries) {
        List<KVUnit> kvs = new ArrayList<>(50000);
        Random random = new Random();
        for (int i = 0; i < numberOfEntries; i++) {
            byte[] key = new byte[500];
            byte[] value = new byte[500];
            random.nextBytes(key);
            random.nextBytes(value);
            kvs.add(new KVUnit(key, value));
        }
        return kvs;
    }
}

class KVUnit {
    public static final byte DELETE = 1;
    public static final byte PRESENT = 0;
    private final byte[] key;
    private final byte[] value;
    private final byte isDelete;

    public KVUnit(byte[] key, byte isDelete) {
        Objects.requireNonNull(key);
        this.key = key;
        this.isDelete = isDelete;
        this.value = null;
    }

    public KVUnit(byte[] key, byte[] value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        this.key = key;
        this.value = value;
        this.isDelete = PRESENT;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public byte getIsDelete() {
        return isDelete;
    }

    public long unitSize() {
        return key.length + (value != null ? value.length : 0);
    }

    @Override
    public String toString() {
        return "KVUnit{" +
                "key=" + Arrays.toString(key) +
                ", value=" + (value != null ? Arrays.toString(value) : 0) +
                ", isDelete=" + isDelete +
                '}';
    }
}
