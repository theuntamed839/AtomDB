package Tools;

import Checksum.CheckSum;
import sst.Header;
import sst.MiddleBlock;
import sst.ValueUnit;
import util.SizeOf;
import util.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class Validate {
    private File file;
    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
    public Validate(File compactionFile) {
        this.file = compactionFile;
    }

    public void isValid() throws IOException {
        try(FileInputStream outputStream = new FileInputStream(file);
            FileChannel channel = outputStream.getChannel();
        ) {
            Header header = Header.getHeader(file.toPath().toString(), channel, byteBuffer);

            List<Long> myPointers = new ArrayList<>((int) header.getEntries());

            TreeSet<byte[]> set = new TreeSet<>(Arrays::compare);
            ArrayList<byte[]> order = new ArrayList<>((int) header.getEntries());

            Map.Entry<byte[], ValueUnit> previous = null;
            for (int i = 0; i < header.getEntries(); i++) {
                myPointers.add(channel.position());
                Map.Entry<byte[], ValueUnit> valueUnitEntry = readKeyValue(channel, byteBuffer);
                if (previous == null) {
                    previous = valueUnitEntry;
                } else if (Arrays.compare(previous.getKey(), valueUnitEntry.getKey()) >= 0) {
                    System.out.println("fucked previous value greateer".repeat(20));
                }

                set.add(valueUnitEntry.getKey());
                order.add(valueUnitEntry.getKey());
            }

            List<Long> pointers = MiddleBlock.readPointers(channel,
                    byteBuffer,
                    header.getBinarySearchLocation(),
                    new ArrayList<>((int) header.getEntries()),
                    header.getEntries());

            Util.requireEquals(pointers.size(), myPointers.size(), "pointer size differet");

            for (int i = 0; i < pointers.size(); i++) {
                Util.requireEquals(pointers.get(i), myPointers.get(i), "pointervalue mismatch");
            }

            Util.requireEquals(set.size(), order.size(), "kv differtent");

            Util.requireTrue(Arrays.compare(
                    set.first(), header.getSmallestKey()) == 0, "first value differetn");

            Util.requireTrue(Arrays.compare(
                    set.last(), header.getLargestKey()) == 0, "last value differetn");


            int i = 0;
            for (byte[] bytes : set) {
                byte[] bytes1 = order.get(i++);
                Util.requireTrue(Arrays.compare(bytes, bytes1) == 0,
                        "mismatch of key\n" + "smK="+ new String(bytes)
                 + " laK=" + new String(bytes1));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] readKey(FileChannel channel, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(SizeOf.LongLength);
        channel.read(byteBuffer);
        byteBuffer.flip();
        int length = (int) byteBuffer.getLong();
        byte[] key = new byte[length];

        byteBuffer.clear();
        byteBuffer.limit(length);
        channel.read(byteBuffer);
        byteBuffer.flip();
        byteBuffer.get(key);
        return key;
    }

    public static Map.Entry<byte[], ValueUnit> readKeyValue(FileChannel channel, ByteBuffer byteBuffer) throws Exception {
        byte[] key = readKey(channel, byteBuffer);

        byteBuffer.clear();
        byteBuffer.limit(Short.BYTES); // for marker , delete or not
        channel.read(byteBuffer);
        byteBuffer.flip();
        short isDelete = byteBuffer.getShort();

        if (isDelete == ValueUnit.DELETE) {
            verifyChecksum0(byteBuffer, channel,key);
            return Map.entry(key, new ValueUnit(ValueUnit.DELETE));
        }

        // the key is not deleted
        // offset + 8 (long bytes) + key length + 2 (short bytes for marker)
        byte[] value = readKey(channel, byteBuffer);

        verifyChecksum(byteBuffer, channel, key, value);

        return Map.entry(key, new ValueUnit(value, isDelete));
    }

    private static void verifyChecksum(ByteBuffer byteBuffer, FileChannel channel, byte[] key, byte[] value) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(Long.BYTES);
        channel.read(byteBuffer);
        byteBuffer.flip();
        long checksum = byteBuffer.getLong();
        if (CheckSum.compute(key, value) != checksum) {
            throw new Exception("Checksum not matching");
        }
    }

    private static void verifyChecksum0(ByteBuffer byteBuffer, FileChannel channel, byte[] key) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(Long.BYTES);
        channel.read(byteBuffer);
        byteBuffer.flip();
        long checksum = byteBuffer.getLong();
        if (CheckSum.compute(key) != checksum) {
            throw new Exception("Checksum not matching");
        }
    }
}
