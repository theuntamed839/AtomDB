package Printer;

import com.google.common.primitives.Longs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import static Constants.DBConstants.EOF;

public class SSTStructurePrinter {
    private static long position = 0;
    private static ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
    private final static int LongLength = Longs.toByteArray(1L).length;
    public static void print(String filePath) throws IOException {
        position = 0;
        byteBuffer.clear();
        System.out.println("readding file " + filePath);
        FileChannel channel = new FileInputStream(new File(filePath)).getChannel();

        printBinaryOffSet(channel);
        printEntryNum(channel);
        printLimits(channel);
    }

    private static void printLimits(FileChannel channel) throws IOException {
        System.out.println("first limit = " + new String(getKey(channel)));
        System.out.println("second limit = " + new String(getKey(channel)));
    }

    private static void printEntryNum(FileChannel channel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, position);
        byteBuffer.flip();
        System.out.println("nums of entry = " +byteBuffer.getLong());
        byteBuffer.clear();
        position += LongLength;
    }

    private static void printBinaryOffSet(FileChannel channel) throws IOException {
        byteBuffer.clear();
        byteBuffer.limit(LongLength);
        channel.read(byteBuffer, position);
        byteBuffer.flip();
        System.out.println("binary offset = " +byteBuffer.getLong());
        byteBuffer.clear();
        position += LongLength;
    }

    private static byte[] getKey(FileChannel channel) throws IOException {
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
}
