package sst;

import Checksum.CheckSum;
import com.google.common.hash.BloomFilter;
import util.SizeOf;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;

public class MiddleBlock {
    public static void writeBlock(FileChannel channel, ByteBuffer byteBuffer, Map.Entry<byte[], ValueUnit> entry) throws IOException {
        byteBuffer.clear();
        byteBuffer.putLong(entry.getKey().length)
                .put(entry.getKey())
                .putShort(entry.getValue().getIsDelete());
        if (entry.getValue().getIsDelete() != ValueUnit.DELETE) {
            byteBuffer.putLong(entry.getValue().getValue().length)
                    .put(entry.getValue().getValue())
                    .putLong(CheckSum.compute(entry.getKey(), entry.getValue().getValue()));
        } else {
            byteBuffer.putLong(CheckSum.compute(entry.getKey()));
        }
        byteBuffer.flip();
        channel.write(byteBuffer);
    }

    public static void writePointers(FileChannel channel, ByteBuffer byteBuffer, List<Long> pointers) throws Exception {
        byteBuffer.clear();
        int limit = byteBuffer.limit();
        limit = (limit / SizeOf.LongLength) - 1;

        for(int i = 0; i < pointers.size(); ) {
            for (int j = 0; j < limit &&
                    i < pointers.size() &&
                    byteBuffer.remaining() > SizeOf.LongLength; j++, i++) {

                byteBuffer.putLong(pointers.get(i));
            }
            byteBuffer.flip();
            channel.write(byteBuffer);
            byteBuffer.compact();
        }
        if (byteBuffer.position() != 0) {
            throw new Exception("pointers not written fully");
        }
        // sure code that works
//        byteBuffer.clear();
//        for (Long pointer : pointers) {
//            byteBuffer.clear()
//                    .putLong(pointer)
//                    .flip();
//            channel.write(byteBuffer);
//        }
    }

    /**
     * // todo
     *  adding footer to contain the meta data about the sst
     *  like total size of that sst and maybe a checksum
     * // todo
     * further reading pointer can be improved by getting only
     * limited amount of pointers and not reading them all at once
     *
     */
    public static List<Long> readPointers(FileChannel channel,
                                          ByteBuffer byteBuffer,
                                          long binarySearchLocation,
                                          List<Long> searchList,
                                          long entries) throws IOException {
        long totalToRead = entries * Long.BYTES;
        byteBuffer.clear();
        int limit = byteBuffer.limit();
        limit = (int) Math.min(
                (limit / Long.BYTES) * Long.BYTES,
                totalToRead);
        byteBuffer.limit(limit);
        channel.position(binarySearchLocation);
        // todo
        // can go into infinite loop
        while(totalToRead > 0) {
            channel.read(byteBuffer);
            totalToRead -= limit;
            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                searchList.add(byteBuffer.getLong());
            }
            byteBuffer.compact();
            limit = (int) Math.min(
                    limit,
                    totalToRead);
            byteBuffer.limit(limit);
        }
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            searchList.add(byteBuffer.getLong());
        }

        if (searchList.get(0) == 0) {
            throw new EOFException("found zero in sst binary search");
        }

        return searchList;

        // the code that works surely
//        long offset = binarySearchLocation;
//        byteBuffer.clear();
//        byteBuffer.limit(8);
//        while(channel.read(byteBuffer, offset) != -1) {
//            byteBuffer.flip();
//            long index = byteBuffer.getLong();
//            if (index == 0) {
//                System.out.println("found at position=" + offset);
//            }
//            searchList.add(index);
//            byteBuffer.clear();
//            byteBuffer.limit(8);
//            offset += 8;
//        }
//
//        // hack
//        if (searchList.get(0) == 0) {
//            throw new EOFException("found zero in sst binary search");
//        }
//
//        return searchList;
    }

    public static byte[] readKey(FileChannel channel, ByteBuffer byteBuffer, Long offset) throws IOException {
        byteBuffer.clear();
        channel.position(offset);
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

    public static Map.Entry<byte[], ValueUnit> readKeyValue(FileChannel channel, ByteBuffer byteBuffer, Long offset) throws Exception {
        byte[] key = readKey(channel, byteBuffer, offset);

        byteBuffer.clear();
        byteBuffer.limit(Short.BYTES); // for marker , delete or not
        channel.read(byteBuffer, offset + Long.BYTES + key.length);
        byteBuffer.flip();
        short isDelete = byteBuffer.getShort();

        if (isDelete == ValueUnit.DELETE) {
            verifyChecksum0(byteBuffer, channel, offset + Long.BYTES + key.length + Short.BYTES
                    ,key);
            return Map.entry(key, new ValueUnit(ValueUnit.DELETE));
        }

        // the key is not deleted
        // offset + 8 (long bytes) + key length + 2 (short bytes for marker)
        byte[] value = readKey(channel, byteBuffer, offset + Long.BYTES + key.length + Short.BYTES);

        long checksumPosition = offset + Long.BYTES + key.length + Short.BYTES + Long.BYTES + value.length;
        verifyChecksum(byteBuffer, channel, checksumPosition, key, value);

        return Map.entry(key, new ValueUnit(value, isDelete));
    }

    private static void verifyChecksum(ByteBuffer byteBuffer, FileChannel channel,long position, byte[] key, byte[] value) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(Long.BYTES);
        channel.read(byteBuffer, position); // previously it was channel.read(byteBuffer);
        byteBuffer.flip();
        long checksum = byteBuffer.getLong();
        if (CheckSum.compute(key, value) != checksum) {
            throw new Exception("Checksum not matching");
        }
    }

    private static void verifyChecksum0(ByteBuffer byteBuffer, FileChannel channel,long position, byte[] key) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(Long.BYTES);
        channel.read(byteBuffer, position);// previously it was channel.read(byteBuffer);
        byteBuffer.flip();
        long checksum = byteBuffer.getLong();
        if (CheckSum.compute(key) != checksum) {
            throw new Exception("Checksum not matching");
        }
    }

    public static void writeBloom(FileOutputStream outputStream, BloomFilter<byte[]> filter) throws IOException {
        filter.writeTo(outputStream);
    }
}
