package Compaction;

import com.google.common.primitives.Longs;
import static Constants.DBConstants.EOF;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class Helper implements Comparable<Helper>{
    private long position;
    private FileChannel channel;
    private final static int LongLength = Longs.toByteArray(1L).length;

    // optimization oppu, no need of buffer for every helper
    private static final ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
    private boolean isEndOfFile = false;
    private long binarySearchPosition = -1;
    private byte[] lowLimit, highLimit;
    public boolean isEOF() {
        return position == binarySearchPosition;
    }
    private long currentCountOfRetrivedKey = 0;
    public Helper(FileChannel channel, String filename) throws IOException {
        this.channel = channel;
        this.position = 0;
        // getting binary serch index
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, position);
        byteBuffer.flip();
        binarySearchPosition = byteBuffer.getLong();
        byteBuffer.clear();
        position += LongLength;

        System.out.println("binary search position " + binarySearchPosition);
        System.out.println("filename = "+ filename);
        lowLimit = getKey();
        highLimit = getKey();
    }

    public byte[] getLowLimit() {
        return lowLimit;
    }

    public byte[] getHighLimit() {
        return highLimit;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public FileChannel getChannel() {
        return channel;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
    }

    private byte[] getKey() throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            isEndOfFile = true;
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

    private byte[] getValue() throws IOException {
        return getKey();
    }

    public byte[][] getKeyValue() throws IOException {
        if (position == binarySearchPosition) {
            throw new EOFException("currentCountOfRetrivedKey >= entriesInSST so the file is over");
        }
        currentCountOfRetrivedKey++;
        return new byte[][]{
            getKey(), getValue()
        };
    }

    public boolean hasEntry() {
        return position != binarySearchPosition;
    }

    public byte[] peekKey() throws IOException {
        if (isEOF()) {
            return EOF;
        }
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        long startPosition = position;

        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            isEndOfFile = true;
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

        // moving to start of that key;
        position = startPosition;
        return keyOne;
    }

    private byte[] skipValue() throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            isEndOfFile = true;
            return EOF;
        }
        position += LongLength;

        byteBuffer.flip();
        long valueLen = byteBuffer.getLong();
        byteBuffer.clear();

        position += valueLen;
        return null;
    }

    @Override
    public int compareTo(Helper o) {
        byte[] providedKey ;
        try {
            if (!o.hasEntry()) {
                if (!this.hasEntry()) {
                    System.out.print("both empty".repeat(500));
                    return 0;
                } else {
                    System.out.print("provided empty but this not empty".repeat(500));
                    return 1;
                }
            }
            providedKey = o.peekKey();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.print("exception for provided".repeat(500));
            return 1;
        }
        byte[] thisKey ;
        try {
            thisKey = this.peekKey();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.print("exception for this".repeat(500));
            return -1;
        }
        if (Arrays.compare(providedKey, EOF) == 0) {
            if (Arrays.compare(thisKey, EOF) == 0) {
                System.out.print("EOF".repeat(10));
                System.out.println();
                return 0;
            } else{
                System.out.print("provided key EOF".repeat(500));
                return 1;
            }
        }

        return Arrays.compare(thisKey, providedKey);
    }
    // helping metthod
    public static long keyValueBlock(byte[][] keyValue, ByteBuffer byteBuffer) {
        byte[] key = keyValue[0];
        byte[] value = keyValue[1];
        byteBuffer.clear();
        byteBuffer.putLong(key.length);byteBuffer.put(key);
        byteBuffer.putLong(value.length);byteBuffer.put(value);
        byteBuffer.flip();
        return (LongLength * 2L) + key.length + value.length;
    }

    public byte[] getElement() throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);

        int eof = channel.read(byteBuffer, position);
        if (eof == -1) {
            isEndOfFile = true;
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
}
