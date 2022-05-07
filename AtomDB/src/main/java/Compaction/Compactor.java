package Compaction;

import Printer.Checker;
import com.google.common.primitives.Longs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import static Constants.DBConstants.EOF;

public class Compactor {
    private final static int LongLength = Longs.toByteArray(1L).length;
    private final String folder;
    private List<String> files;
    private static final ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
    private long position = 0;
    public Compactor(String folder, List<String> files) {
        Objects.requireNonNull(files);
        this.files = files;
        this.folder = folder;
    }

    public long compact(FileChannel channel) throws Exception {
        position = 0;
        List<Helper> helperList = getHelperList(files);
        byte[][] firstAndLast = getFirstAndLast(helperList);
        position += writeBinaryOffset(channel, 123L); // writing garbage at binarysearch offset position
        writeKV(firstAndLast, channel);
//        Deque<Helper> qu = new ArrayDeque<>(helperList);
        PriorityQueue<Helper> qu = new PriorityQueue<>(helperList);
        List<Long> keyOffsets = new ArrayList<>();
        while (!qu.isEmpty()) {
            Helper first = qu.remove();
            if (!first.hasEntry()) {
                continue;
            }
//            System.out.println("adding position" + position);

            byte[][] keyValue = first.getKeyValue();
            keyOffsets.add(position);
            writeKV(keyValue, channel);
            if (first.hasEntry()) {
                qu.add(first);
            }
        }
        writeBinaryOffset(channel, position);
//        System.out.println("writing offsets");
//        System.out.println(keyOffsets);
        writeKeyOffsets(channel, keyOffsets);
//        System.out.println("comp: writing low " + new String(firstAndLast[0]) + " high " + new String(firstAndLast[1]));
//        System.out.println("binayr = " + position);
        channel.close();
        return position;
    }

    private void writeKeyOffsets(FileChannel channel, List<Long> keyOffsets) throws IOException {
        for (Long offset : keyOffsets) {
//            if (offset == 67364 ) {
//                throw  new EOFException("zero in offsets");
//            }
            if (offset == 0 ) {
                throw  new EOFException("zero in offsets");
            }
            byteBuffer.clear();
            byteBuffer.putLong(offset);
            byteBuffer.flip();
            position += channel.write(byteBuffer, position);
        }
    }

    private long writeNumberOfEntries(FileChannel channel, long value) throws IOException {
        byteBuffer.clear();
        byteBuffer.putLong(value);
        byteBuffer.flip();
        channel.write(byteBuffer, LongLength);
        return LongLength;
    }

    public long writeBinaryOffset(FileChannel channel, long value) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        byteBuffer.putLong(value);
        byteBuffer.flip();
        channel.write(byteBuffer, 0);
        return LongLength;
    }

    private byte[][] getFirstAndLast(List<Helper> helperList) {
        byte[] first = helperList.stream()
                .map(Helper::getLowLimit)
                .filter(each -> Arrays.compare(EOF, each) != 0)
                .min(Arrays::compare)
                .get();
        byte[] second = helperList.stream()
                .map(Helper::getHighLimit)
                .filter(each -> Arrays.compare(EOF, each) != 0)
                .max(Arrays::compare)
                .get();
        return new byte[][] {
                first, second
        };
    }

    private List<Helper> getHelperList(List<String> files) throws IOException {
        String filename = "";
        try{
            List<Helper> list = new ArrayList<>(files.size());
            for (String file : files) {
//            Printer.SSTStructurePrinter.print(folder +
//                    System.getProperty("file.separator")+ file);
                filename = file;
                list.add(
                        new Helper(new FileInputStream(folder +
                                System.getProperty("file.separator") + file).getChannel(), folder +
                                System.getProperty("file.separator") + file)
                );
            }
            return list;
        } catch (Exception e) {
            System.out.println("exception for " + filename);
            throw e;
        }
    }

    private long writeKV(byte[][] keyValue, FileChannel channel) throws IOException {
        long size = Helper.keyValueBlock(keyValue, byteBuffer);
        channel.write(byteBuffer, position);
        position += size;
        byteBuffer.clear();
        return size;
    }


    private Map<FileChannel, Integer> getFileChangePointerMap() throws Exception {
        Map<FileChannel, Integer> channels = new HashMap<>(files.size());
        for (String fileName : files) {
            var channel = new RandomAccessFile(new File(fileName), "r")
                    .getChannel();
            int pointer = alignPointerToFirstValue(channel);
            if (pointer == -1) {
                throw new Exception("");
            }
            channels.put(channel, pointer);
        }
        return channels;
    }

    private int alignPointerToFirstValue(FileChannel channel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        int position = 0;
        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            return eof;
        }
        position += LongLength;

        byteBuffer.flip();
        long keyLen = byteBuffer.getLong();
        position += keyLen;
        byteBuffer.clear();

        channel.read(byteBuffer, position);

        byteBuffer.flip();
        keyLen = byteBuffer.getLong();
        position += keyLen;
        return position;
    }
}
