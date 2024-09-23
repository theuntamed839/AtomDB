package sst;

import Checksum.CheckSumStatic;
import com.google.common.hash.BloomFilter;
import sstIo.ReaderInterface;
import sstIo.PrimitiveWriter;
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
                    .putLong(CheckSumStatic.compute(entry.getKey(), entry.getValue().getValue()));
        } else {
            byteBuffer.putLong(CheckSumStatic.compute(entry.getKey()));
        }
        byteBuffer.flip();
        channel.write(byteBuffer);
    }

    public static void writeMiddleBlock(PrimitiveWriter writer, byte[] key, ValueUnit value) {
        if (value.getIsDelete() == ValueUnit.DELETE) {
            writer.putInt(key.length)
                    .putBytes(key)
                    .putLong(CheckSumStatic.compute(key))
                    .putByte(value.getIsDelete());
        } else {
            writer.putInt(key.length)
                    .putBytes(key)
                    .putLong(CheckSumStatic.compute(key))
                    .putByte(value.getIsDelete())
                    .putInt(value.getValue().length)
                    .putBytes(value.getValue())
                    .putLong(CheckSumStatic.compute(key, value.getValue()));
        }
    }

    public static void writePointers(FileChannel channel, ByteBuffer byteBuffer, List<Long> pointers) throws Exception {
        byteBuffer.clear();
        for (Long pointer : pointers) {
            if (byteBuffer.remaining() >= SizeOf.LongLength) {
                byteBuffer.putLong(pointer);
            } else {
                byteBuffer.flip();
                channel.write(byteBuffer);
                byteBuffer.clear();
                byteBuffer.putLong(pointer);
            }
            //instead of compact, clear can be used. or we can check for remaining() if > 0 then compact else clear()
        }
        byteBuffer.flip();
        channel.write(byteBuffer);
        byteBuffer.clear();

        if (byteBuffer.position() != 0) {
            throw new Exception("pointers not written fully");
        }
    }

    public static void writePointers(PrimitiveWriter writer, List<Long> pointers) {
        // todo can be improved.
        for (Long pointer : pointers) {
            writer.putLong(pointer);
        }
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

        return Map.entry(key, new ValueUnit(value, (byte) isDelete));
    }

    private static void verifyChecksum(ByteBuffer byteBuffer, FileChannel channel,long position, byte[] key, byte[] value) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(Long.BYTES);
        channel.read(byteBuffer, position); // previously it was channel.read(byteBuffer);
        byteBuffer.flip();
        long checksum = byteBuffer.getLong();
        if (CheckSumStatic.compute(key, value) != checksum) {
            throw new Exception("Checksum not matching");
        }
    }

    private static void verifyChecksum0(ByteBuffer byteBuffer, FileChannel channel,long position, byte[] key) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(Long.BYTES);
        channel.read(byteBuffer, position);// previously it was channel.read(byteBuffer);
        byteBuffer.flip();
        long checksum = byteBuffer.getLong();
        if (CheckSumStatic.compute(key) != checksum) {
            throw new Exception("Checksum not matching");
        }
    }

    public static void writeBloom(FileOutputStream outputStream, BloomFilter<byte[]> filter) throws IOException {
        filter.writeTo(outputStream);
    }

    public static KeyUnit getKeyUnit(ReaderInterface reader, long position) {
        ByteBuffer byteBuffer = reader.readSize(new byte[SizeOf.IntLength], position, SizeOf.IntLength);
        int keySize = byteBuffer.getInt();
        int readSize = keySize + SizeOf.LongLength +  Byte.BYTES + SizeOf.IntLength;
        byteBuffer = reader.readSize(new byte[readSize], readSize);
        byte[] key = new byte[keySize];
        byteBuffer.get(key);
        long checkSum = byteBuffer.getLong();
        byte isDelete = byteBuffer.get();
        if (isDelete != KeyUnit.DELETE) {
            return new KeyUnit(key, checkSum, isDelete, byteBuffer.getInt());
        }
        return new KeyUnit(key, checkSum, isDelete, -1);
    }

    public static byte[] getValueUnit(ReaderInterface reader, long position, KeyUnit keyUnit) {
        int valueSize = keyUnit.getValueSize();
        ByteBuffer byteBuffer = reader.readSize(new byte[valueSize + SizeOf.LongLength],
                position + SizeOf.IntLength + keyUnit.getKey().length + SizeOf.LongLength + Byte.BYTES + SizeOf.IntLength,
                valueSize + SizeOf.LongLength);
        byte[] value = new byte[valueSize];
        byteBuffer.get(value);
        long checkSum = byteBuffer.getLong();
        if (CheckSumStatic.compute(keyUnit.getKey(), value) != checkSum) {
            throw new RuntimeException("Checksum mismatch");
        }
        return value;
    }

    public static KeyUnit getKeyUnit(byte[] bytes, ValueUnit valueUnit) {
        return new KeyUnit(bytes, CheckSumStatic.compute(bytes), valueUnit.getIsDelete(),
                valueUnit.getIsDelete() != ValueUnit.DELETE ? valueUnit.getValue().length : -1);
    }
}