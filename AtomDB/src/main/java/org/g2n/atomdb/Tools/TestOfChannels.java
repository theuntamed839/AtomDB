package org.g2n.atomdb.Tools;

import org.g2n.atomdb.Checksum.CheckSumStatic;
import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.sst.Header;
import org.g2n.atomdb.sst.ValueUnit;
import org.g2n.atomdb.util.BytesConverter;
import org.g2n.atomdb.util.SizeOf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TestOfChannels {
    private static final int BS_POSITION = 16;
    private static final int ENTRIES_POSITION = 24;
    private static final int HEADER_POSITION = 0;


    public static void main(String[] args) throws Exception {
        var map = new ConcurrentSkipListMap<byte[], ValueUnit>(DBComparator.byteArrayComparator);
        String value = "hello".repeat(50);
        for (int i = 0; i < 10000; i++) {
            map.put(BytesConverter.bytes(i + ""),
                    new ValueUnit(BytesConverter.bytes(value), ValueUnit.ADDED));
        }

        createSST(map, ByteBuffer.allocate(4096));
        System.out.println("creating ");
        createSSTRand(map);
    }

    public static void createSST(SortedMap<byte[], ValueUnit> map, ByteBuffer byteBuffer) throws Exception {
        String tempFileName = Instant.now().toString().replace(':', '_') + Level.LEVEL_ZERO;

        var header = new Header(map.firstKey(), map.lastKey(), map.size(), DBConstant.SST_VERSION, Level.LEVEL_ZERO, tempFileName);

        try(FileOutputStream outputStream = new FileOutputStream(tempFileName);
            FileChannel channel = outputStream.getChannel();) {
            channel.force(false);
            writeHeader(header, channel, byteBuffer);

            List<Long> pointers = new ArrayList<>(map.size());
            for (Map.Entry<byte[], ValueUnit> entry : map.entrySet()) {
                pointers.add(channel.position());
                writeBlock(channel, byteBuffer, entry);
            }

            long bs = channel.position();
            writePointers(channel, byteBuffer, pointers);

            writeBS(header, channel, byteBuffer, bs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // debug
        var val = new Validate(new File(tempFileName));
        val.isValid();
    }

    public static void createSSTRand(SortedMap<byte[], ValueUnit> map) throws Exception {
        String tempFileName = Instant.now().toString().replace(':', '_') + Level.LEVEL_ZERO;

        var header = new Header(map.firstKey(), map.lastKey(), map.size(), DBConstant.SST_VERSION, Level.LEVEL_ZERO, tempFileName);

        try(RandomAccessFile channel = new RandomAccessFile(
                tempFileName, "rw")) {

            System.out.println("writing header");
            writeHeaderRand(header, channel);

            System.out.println("writing block middle");
            List<Long> pointers = new ArrayList<>(map.size());
            for (Map.Entry<byte[], ValueUnit> entry : map.entrySet()) {
                pointers.add(channel.getFilePointer());
                writeBlockRand(channel, entry);
            }

            long bs = channel.getFilePointer();
            System.out.println("writing pointers");
            writePointersRand(channel, pointers);

            System.out.printf("writing bs poisiton");
            writeBSRand(header, channel, bs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // debug
        System.out.println("testing validate");
        var val = new Validate(new File(tempFileName));
        val.isValid();
    }

    public static void writeHeader(Header header, FileChannel channel, ByteBuffer byteBuffer) throws Exception{
        byteBuffer.clear();
        byteBuffer.putLong(header.getVersionId())
                .putLong(Level.toID(header.getLevel()))
                .putLong(header.getBinarySearchLocation())
                .putLong(header.getEntries())
                .putLong(header.getSmallestKey().length)
                .put(header.getSmallestKey())
                .putLong(CheckSumStatic.compute(header.getSmallestKey()))
                .putLong(header.getLargestKey().length)
                .put(header.getLargestKey())
                .putLong(CheckSumStatic.compute(header.getLargestKey()))
                .flip();
        channel.position(0); // moved to 0 position
        channel.write(byteBuffer);
    }

    public static void writeHeaderRand(Header header, RandomAccessFile channel) throws Exception{
        channel.writeLong(header.getVersionId());
        channel.writeLong(Level.toID(header.getLevel()));
        channel.writeLong(header.getBinarySearchLocation());
        channel.writeLong(header.getEntries());
        channel.writeLong(header.getSmallestKey().length);
        channel.write(header.getSmallestKey());
        channel.writeLong(CheckSumStatic.compute(header.getSmallestKey()));
        channel.writeLong(header.getLargestKey().length);
        channel.write(header.getLargestKey());
        channel.writeLong(CheckSumStatic.compute(header.getLargestKey()));
    }

    public static void writeBS(Header header, FileChannel channel, ByteBuffer byteBuffer, long binarySearchLocation) throws IOException {
        byteBuffer.clear();
        byteBuffer.putLong(binarySearchLocation)
                .flip();
        channel.write(byteBuffer, BS_POSITION);
    }

    public static void writeBSRand(Header header, RandomAccessFile channel, long binarySearchLocation) throws IOException {
        channel.seek(BS_POSITION);channel.writeLong(binarySearchLocation);
    }

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

    public static void writeBlockRand(RandomAccessFile channel, Map.Entry<byte[], ValueUnit> entry) throws IOException {
         channel.writeLong(entry.getKey().length);
         channel.write(entry.getKey());
         channel.writeShort(entry.getValue().getIsDelete());

        if (entry.getValue().getIsDelete() != ValueUnit.DELETE) {
            channel.writeLong(entry.getValue().getValue().length);
            channel.write(entry.getValue().getValue());
            channel.writeLong(CheckSumStatic.compute(entry.getKey(), entry.getValue().getValue()));
        } else {
            channel.writeLong(CheckSumStatic.compute(entry.getKey()));
        }
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

    // todo can be made with channel directly, need benchmark
    public static void writePointersRand(RandomAccessFile channel, List<Long> pointers) throws Exception {

        var buff = ByteBuffer.allocate(pointers.size() * Long.BYTES);
        for (Long pointer : pointers) {
            buff.putLong(pointer);
        }
        channel.write(buff.array());
        // sure code that works
//        byteBuffer.clear();
//        for (Long pointer : pointers) {
//            byteBuffer.clear()
//                    .putLong(pointer)
//                    .flip();
//            channel.write(byteBuffer);
//        }
    }
}
