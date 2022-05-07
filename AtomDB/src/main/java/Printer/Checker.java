package Printer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static Constants.DBConstants.EOF;
import static Constants.DBConstants.LongLength;

public class Checker {
    private FileChannel channel;
    private ByteBuffer byteBuffer;
    private long binary, entries;
    private Map<String, String> map;
    private long position = 0;
    private byte[] first, last;
    private long startOffset;
    public Checker(FileChannel channel) throws IOException {
        System.out.println("checker");
        this.channel = channel;
        byteBuffer = ByteBuffer.allocate(4096);
//        binary = getBinaryOffset();
//        first = getElement(LongLength * 2L);
//        last = getElement(LongLength * 3L + first.length);
//        startOffset = LongLength * 4L + first.length + last.length;

//        System.out.println("entries by reading " + entries);
//        System.out.println("startOffset cal=" + startOffset);
//        System.out.println("binaryOffsetPointsTo = " + binary);
//        System.out.println("first key" + new String(first));
//        System.out.println("last key" + new String(last));
//        map = new HashMap<>();
//        keyValuesByBinarySearch();
//        keysByLinear();
//        printSSt();
    }

    public void keysByLinear() throws IOException {
        long tempOffset = startOffset;
        int count = 0;
        System.out.println("biunary in linar ="+binary);
        for (int i = 0; i < entries; i++) {
            byteBuffer.clear();
            byteBuffer.limit(LongLength);
            if (channel.read(byteBuffer, tempOffset) == -1) {
                System.out.println("tempOffset ="+tempOffset);
                System.out.println("fucked up");
            }
            tempOffset += LongLength;
            byteBuffer.flip();
            long length = byteBuffer.getLong();
            if (length == binary) {
                System.out.println("the end");
                System.out.println("map size" + map.size());
            }
            byteBuffer.clear();
            byteBuffer.limit((int) length);
            channel.read(byteBuffer, tempOffset);
            tempOffset += length;
            byteBuffer.flip();
            var keyBytes = new byte[(int) length];
            byteBuffer.get(keyBytes);
            System.out.println("key =" + new String(keyBytes));
            byteBuffer.clear();
            byteBuffer.limit(LongLength);
            channel.read(byteBuffer, tempOffset);
            tempOffset += LongLength;
            byteBuffer.flip();
            length = byteBuffer.getLong();
            byteBuffer.clear();
            byteBuffer.limit((int) length);
            channel.read(byteBuffer, tempOffset);
            tempOffset += length;
            byteBuffer.flip();
            var valueBytes = new byte[(int) length];
            byteBuffer.get(valueBytes);
            if (!map.containsKey(keyBytes)) {
                System.out.println("kv which didnt get from binary");
                System.out.println("k =" + new String(keyBytes) + " v=" + new String(valueBytes));
                System.out.println("current count " + count);
            } else {
                System.out.println("cotained already");
            }
            count++;
        }
    }

    public void keyValuesByBinarySearch() throws IOException {
        List<Long> offsets = getOffsets();
        System.out.println(offsets);
        System.out.println("entries by reading " + entries);
        System.out.println("entries by offsets " + offsets.size());
        System.out.println("startOffset cal=" + startOffset);
        System.out.println("According to binary Offset=" + offsets.get(0));
        for (Long offset : offsets) {
            byte[] key = getElement(offset);
            byte[] value = getElement(offset);
            if (map.containsKey(new String(key))) {
                System.out.println("mf mf mfm mfmfm m fmfm fmmfmsfmmf");
            }
            map.put(new String(key), new String(value));
        }
    }

    private byte[] getElement(long offset) throws IOException {
        long tempOffset = offset;
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, tempOffset);
        tempOffset += LongLength;
        byteBuffer.flip();
        long length = byteBuffer.getLong();
        byteBuffer.clear();
        byteBuffer.limit((int) length);
        channel.read(byteBuffer, tempOffset);
        tempOffset += length;
        byteBuffer.flip();
        var bytes = new byte[(int) length];
        byteBuffer.get(bytes);
        return bytes;
    }

    public List<Long> getOffsets() throws IOException {
        List<Long> offsets = new ArrayList<>();
        long tempOffset = binary;
        while(true){
            byteBuffer.clear();
            byteBuffer.limit(LongLength);
            if (channel.read(byteBuffer, tempOffset) == -1) {
                break;
            }
            tempOffset += LongLength;
            byteBuffer.flip();
            offsets.add(byteBuffer.getLong());
        }
        return offsets;
    }

    private long getEntriesNumber() throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, LongLength);
        byteBuffer.flip();
        long value = byteBuffer.getLong();
        byteBuffer.clear();
        return value;
    }

    private long getBinaryOffset() throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, 0);
        byteBuffer.flip();
        long value = byteBuffer.getLong();
        byteBuffer.clear();
        return value;
    }

    public void printSSt() throws IOException {
        //binary index
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, position);
        position += LongLength;
        byteBuffer.flip();
        long binaryIndex  = byteBuffer.getLong();
        System.out.println(binaryIndex);

        System.out.println("first limit="+ new String(getAtom()));
        System.out.println("last limit="+ new String(getAtom()));

        while (position != binaryIndex) {
            var tempCurrentPositionBeforeKey = position;
            byte[] key = getAtom();
            var tempCurrentPositionAfterKey = position;
            byte[] value = getAtom();
            System.out.println("position="+tempCurrentPositionBeforeKey +
                    " key=" + new String(key) +
                    " position=" + tempCurrentPositionAfterKey +
                    " value=" + new String(value));
        }

        long tempPositionBeforeRead = position;
        byteBuffer.clear();byteBuffer.limit(LongLength);

        while(channel.read(byteBuffer, position) != -1) {
            byteBuffer.flip();
            System.out.println("position=" + tempPositionBeforeRead + " index="+byteBuffer.getLong());
            position += LongLength;
            tempPositionBeforeRead = position;
            byteBuffer.clear();byteBuffer.limit(LongLength);
        }
        System.out.println("done re nigga");
    }

    public byte[] getAtom() throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, position);
        position += LongLength;
        byteBuffer.flip();
        long length = byteBuffer.getLong();
        System.out.println("length="+length);

        byteBuffer.clear();byteBuffer.limit((int) length);
        channel.read(byteBuffer, position);
        position += length;
        byteBuffer.flip();
        byte[] arr = new byte[(int) length];
        byteBuffer.get(arr);
        return arr;
    }
}
