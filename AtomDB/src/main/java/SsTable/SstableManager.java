package SsTable;

import Compaction.Compaction;
import Printer.Checker;
import Table.Table;
import Compaction.Level;
import com.google.common.primitives.Longs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class SstableManager {
    private final static int LongLength = Longs.toByteArray(1L).length;
    private String folder;
    private ByteBuffer byteBuffer;
    private final byte[] LongByteArr = new byte[LongLength];
    private long position  = 0;
    private final byte[] EOF = new byte[] {-1, -1, -1, -1, -1, -1, -1};
    private Compaction compaction;
    private Table table;
    private long binarySearchValueOffset = -1;
    public SstableManager(String folder) {
        table = Table.getTable(folder);
        new File(folder).mkdir();
        this.folder = folder;
        byteBuffer = ByteBuffer.allocateDirect(4096);
        compaction = new Compaction(table);
    }

    public void makeSstable(ConcurrentSkipListMap<byte[], byte[]> map) throws Exception {
        File file = table.getNewFile(Level.LEVEL_ZERO);
        FileOutputStream fileOutputStream = new FileOutputStream(
                file);
        FileChannel channel = fileOutputStream.getChannel();
        long fileSize = 0;
        // writing garbage at binarysearch offset position
        byteBuffer.clear();
        byteBuffer.putLong(123L);
        byteBuffer.flip();
        channel.write(byteBuffer);
        fileSize += LongLength;

        byte[] first = map.firstEntry().getKey();
        byte[] last = map.lastEntry().getKey();
        byteBuffer.clear();
        byteBuffer.putLong(first.length);byteBuffer.put(first);
        fileSize += LongLength + first.length;
        byteBuffer.putLong(last.length);byteBuffer.put(last);
        fileSize += LongLength + last.length;
        byteBuffer.flip();
        channel.write(byteBuffer);
        byteBuffer.clear();
        List<Long> offsets = new ArrayList<>(map.size());
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            offsets.add(fileSize);
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            byteBuffer.putLong(key.length);byteBuffer.put(key);
            byteBuffer.putLong(value.length);byteBuffer.put(value);
            byteBuffer.flip();
            channel.write(byteBuffer);
            byteBuffer.clear();
            fileSize += LongLength * 2L + key.length + value.length;
        }
        fileSize += writeBinarySearchLayout(channel, offsets, fileSize);


        channel.close();
        fileOutputStream.close();
        table.putInInitialLevel(fileSize, file.getName());
//        System.out.println("binarysearch index " + offsets);
//        System.out.println("number of entries " + map.size());
        System.out.println("written " + file.getName());

//        FileChannel channel1 = new FileInputStream(file).getChannel();
//        new Checker(channel1);
//        channel1.close();
        //
//        RandomAccessFile rand = new RandomAccessFile(file, "r");
//        FileChannel channel1 = rand.getChannel();
//        System.out.println("fucking " + file.getName());
//        var a = new Checker(channel1); // need to remove
//        a.printSSt();
        compaction.compactionMaybe();
    }

    private long writeBinarySearchLayout(FileChannel channel, List<Long> offsets, long fileSize) throws IOException {
        for (Long offset : offsets) {
            byteBuffer.clear();
            byteBuffer.putLong(offset);
            byteBuffer.flip();
            channel.write(byteBuffer);
        }
        byteBuffer.clear();
        byteBuffer.putLong(fileSize);
        byteBuffer.flip();
        channel.write(byteBuffer, 0);
        return (long) offsets.size() * LongLength;
    }

    public byte[] getValue(byte[] keyB, String keyS) throws Exception {
        for (Iterator<String> it = table.getFileIterator(); it.hasNext(); ) {
            String fileName = it.next();
            if (!new File(folder+
                    System.getProperty("file.separator") + fileName).exists()) {
                continue;
            }
            byte[] obj = getValueFromFile(keyB, folder+
                    System.getProperty("file.separator") + fileName);
            if (obj!=null) {
                return obj;
            }

        }
//        for (int i = currentBloomCount - 1; i >= 0; i--) {
//            String fileName = bloomMap.get(i);
//            if (!new File(folder+
//                    System.getProperty("file.separator") + fileName).exists()) {
//                continue;
//            }
//                byte[] obj = getValueFromFile(keyB, folder+
//                        System.getProperty("file.separator") + fileName);
//                if (obj!=null) {
////                    System.out.println("found");
//                    return obj;
//                }
//        }
        System.out.println("no found key" + keyS);
        return null;
    }

    private byte[] getValueFromFile(byte[] keyB, String fileName) throws Exception {
        RandomAccessFile rand = new RandomAccessFile(fileName, "r");
        FileChannel channel = rand.getChannel();
//        System.out.println("fucking " + fileName);
//        var a = new Checker(channel); // need to remove
//        a.printSSt();
//        System.out.println("done fucking");
        position = 0;
//        System.out.println("in getValueFromFile method");
        long binarySearchOff = -1;
        try{
            binarySearchOff = getBinarySearchOffset(channel);
            String keyString = new String(keyB);
            byte[] firstKey = getKey(channel);
            byte[] secondKey = getKey(channel);
//            System.out.println(new String(firstKey) + " first key and second key " + new String(secondKey));
            if (Arrays.compare(firstKey, keyB) == 1) {
                return null;
            }
            if (Arrays.compare(secondKey, keyB) == -1) {
                return null;
            }
//            System.out.println("seems like its in this file");
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println("file name" + fileName);
        }

        try {
//            System.out.println("getting searchList");
            List<Long> searchList = makeListForSearch(channel, binarySearchOff);
            int l = 0, h = searchList.size() -1;
//            System.out.println("going to perform binary");
//            printOffSetWithNumbers(searchList);
            while(l <= h) {
                int mid = (l + h) >>> 1;
                Long offset = searchList.get(mid);
//                System.out.println("mid = " + mid + " size="+ searchList.size());
                byte[] foundKey = getKeyAtOffset(channel, offset, keyB);
//                int compare = Arrays.compare(keyB, foundKey);
                int compare = Arrays.compare(foundKey, keyB);
//                System.out.println("toBeFound=" + new String(keyB)+" keyAtMid=" + new String(foundKey));

//                if (compare == 0) {
//                    System.out.println("found ");
//                    return getValue(channel, binarySearchValueOffset);
//                }else if (compare < 0) {
//                    h = mid - 1;
//                } else {
//                    l = mid + 1;
//                }
                if (compare < 0)
                    l = mid + 1;
                else if (compare > 0)
                    h = mid - 1;
                else
                    return getValue(channel, binarySearchValueOffset);;

//                System.out.println("still in l="+l + " h=" + h);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            System.out.println("file name" + fileName);
        }
        return null;
    }

    private void printOffSetWithNumbers(List<Long> searchList) {
        System.out.print("[");
        for (int i = 0; i < searchList.size(); i++) {
            System.out.print(" "+searchList.get(i) + "=" + i);
        }
        System.out.println("]");
    }

    private byte[] getValue(FileChannel channel, long valueOffset) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, valueOffset);
        if (eof == -1) {
            throw new Exception("was looking for value but file got over");
        }
        valueOffset += LongLength;

        byteBuffer.flip();
        long keyLen = byteBuffer.getLong();
        byteBuffer.clear();

        byteBuffer.limit((int) keyLen);

        channel.read(byteBuffer, valueOffset);
        var keyOne = new byte[(int) keyLen];

        byteBuffer.flip();
        byteBuffer.get(keyOne);
        byteBuffer.clear();
        return keyOne;
    }

    private byte[] getKeyAtOffset(FileChannel channel, Long offset, byte[] keyB) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, offset);
        if (eof == -1) {
            throw new Exception("file over while finding " + new String(keyB));
        }
        offset += LongLength;

        byteBuffer.flip();
        long keyLen = byteBuffer.getLong();
        byteBuffer.clear();

        byteBuffer.limit((int) keyLen);

        channel.read(byteBuffer, offset);
        offset += keyLen;
        binarySearchValueOffset = offset;
        var keyOne = new byte[(int) keyLen];

        byteBuffer.flip();
        byteBuffer.get(keyOne);
        byteBuffer.clear();
        return keyOne;
    }

    private List<Long> makeListForSearch(FileChannel channel, long binarySearchOff) throws IOException {
        long offset = binarySearchOff;
        List<Long> searchList = new ArrayList<>();
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        while(channel.read(byteBuffer, offset) != -1) {
            byteBuffer.flip();
            long index = byteBuffer.getLong();
            if (index == 0) {
                System.out.println("found at position=" + offset);
            }
            searchList.add(index);
            byteBuffer.clear();
            byteBuffer.limit(LongLength);
            offset += LongLength;
        }

        // hack
        if (searchList.get(0) == 0) {
            throw new EOFException("found zero in sst binary search");
        }

        return searchList;
    }

    private long getBinarySearchOffset(FileChannel channel) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, 0);
        if (eof == -1) {
            throw new Exception("fucked up at the start");
        }
        position += LongLength;
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    private long getNumberOfEntries(FileChannel channel) throws Exception {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, LongLength);
        if (eof == -1) {
            throw new Exception("fucked up at the start");
        }
        position += LongLength;
        byteBuffer.flip();
        return byteBuffer.getLong();
    }

    private byte[] skipValue(FileChannel channel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            return EOF;
        }
        position += LongLength;

        byteBuffer.flip();
        long valueLen = byteBuffer.getLong();
        byteBuffer.clear();

        position += valueLen;
        return null;
    }

    private byte[] getKey(FileChannel channel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            return EOF;
        }
        position += LongLength;

        byteBuffer.flip();
        long keyLen = byteBuffer.getLong();
        byteBuffer.clear();

        byteBuffer.limit((int) keyLen);

        channel.read(byteBuffer, position);
        position += keyLen;

        var keyOne = new byte[(int) keyLen];

        byteBuffer.flip();
        byteBuffer.get(keyOne);
        byteBuffer.clear();
        return keyOne;
    }

//    private byte[] getValueFromFile1(byte[] keyB, String fileName) throws IOException {
//        var keyString = new String(keyB);
//        RandomAccessFile rand = new RandomAccessFile(fileName, "r");
//        FileChannel channel = rand.getChannel();
//        long position = 0;
//        byteBuffer.clear();
//        byteBuffer.limit(LongLength);
//        channel.read(byteBuffer, 0);
//        position += LongLength;
//        byteBuffer.flip();
//        long keyLen = byteBuffer.getLong();
//        byteBuffer.clear();
//        byteBuffer.limit((int) keyLen);
//        channel.read(byteBuffer, position);
//        position += keyLen;
//        var keyOne = new byte[(int) keyLen];
//        byteBuffer.flip();
//        byteBuffer.get(keyOne);
//        byteBuffer.clear();
//
//        //second value
//        byteBuffer.limit(LongLength);
//        channel.read(byteBuffer, position);
//        position += LongLength;
//        byteBuffer.flip();
//        keyLen = byteBuffer.getLong();
//        byteBuffer.clear();
//        byteBuffer.limit((int) keyLen);
//        channel.read(byteBuffer, position);
//        position += keyLen;
//        var keyTwo = new byte[(int) keyLen];
//        byteBuffer.flip();
//        byteBuffer.get(keyOne);
//        byteBuffer.clear();
//        var firstKey = new String(keyOne);
//        var secondKey = new String(keyTwo);
//        if (firstKey.compareTo(keyString) > 0) {
//            System.out.println(firstKey + " > " + keyString + " so not found");
//            return null;
//        }
//        if (secondKey.compareTo(keyString) < 0) {
//            System.out.println(secondKey + " < " + keyString + " so not found");
//            return null;
//        }
//        try {
//            int iterated = 0;
//            while (true) {
//                byteBuffer.limit(LongLength);
//                int eof = channel.read(byteBuffer, position);
//                if (eof == -1) {
//                    System.out.println("iterated " + iterated);
//                    return null;
//                }
//                position += LongLength;
//                byteBuffer.flip();
//                keyLen = byteBuffer.getLong();
//                byteBuffer.clear();
//                byteBuffer.limit((int) keyLen);
//                channel.read(byteBuffer, position);
//                position += keyLen;
//                byteBuffer.flip();
//                boolean flag = true;
//                for (int i = 0; i < keyB.length; i++) {
//                    if (keyB[i] != byteBuffer.get(i)) {
//                        flag = false;
//                        break;
//                    }
//                }
//                byteBuffer.clear();
//                byteBuffer.limit(LongLength);
//                channel.read(byteBuffer, position);
//                position += LongLength;
//                byteBuffer.flip();
//                var ValueLen = byteBuffer.getLong();
//                byteBuffer.clear();
//                if (flag) {
//                    byteBuffer.limit((int) ValueLen);
//                    channel.read(byteBuffer, position);
//                    byteBuffer.flip();
//                    var value = new byte[(int) keyLen];
//                    byteBuffer.get(value);
//                    return value;
//                } else {
//                    position += ValueLen;
//                }
//                iterated++;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    //test
//    public String write(ConcurrentSkipListMap<String, Map.Entry<byte[], byte[]>> map) throws IOException {
//        var fileName = LocalDateTime.now().toString();
//        FileOutputStream fileOutputStream = new FileOutputStream(fileName);
//        FileChannel channel = fileOutputStream.getChannel();
//        byte[] first = map.firstEntry().getValue().getKey();
//        byte[] last = map.lastEntry().getValue().getKey();
//        byteBuffer.clear();
//        byteBuffer.putLong(first.length);byteBuffer.put(first);
//        byteBuffer.putLong(last.length);byteBuffer.put(last);
//        byteBuffer.flip();
//        channel.write(byteBuffer);
//        byteBuffer.clear();
//        for (Map.Entry<String, Map.Entry<byte[], byte[]>> entry : map.entrySet()) {
//            byte[] key = entry.getValue().getKey();
//            byte[] value = entry.getValue().getValue();
//            byteBuffer.putLong(key.length);byteBuffer.put(key);
//            byteBuffer.putLong(value.length);byteBuffer.put(value);
//            byteBuffer.flip();
//            channel.write(byteBuffer);
//            byteBuffer.clear();
//        }
//        channel.close();
//        fileOutputStream.close();
//        System.out.println("written ss");
//        return fileName;
//    }

//    public void read(String fileName) throws Exception {
//        RandomAccessFile rand = new RandomAccessFile(fileName, "r");
//        FileChannel channel = rand.getChannel();
//        position = 0;
//        byte[] firstKey = getKey(channel);
//        System.out.println(new String(firstKey));
//        byte[] secondKey = getKey(channel);
//        System.out.println(new String(secondKey));
//
//        try {
//            while (true) {
//                byte[] key = getKey(channel);
//                System.out.println("key "+new String(key));
//                byte[] value = getKey(channel);
//                System.out.println("value " + new String(value));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
