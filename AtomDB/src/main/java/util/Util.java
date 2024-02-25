package util;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.regex.Pattern;

public class Util {

    public static ByteBuffer getExtendedBufferIfNeeded(int length, ByteBuffer buffer) {
//        System.out.println("asking for ByteBufferSize="+length);//debug
//        System.out.println("curBuffCap="+buffer.capacity() + " curBuffLimit="+buffer.limit());
        if (buffer.capacity() <= length) {
            if (buffer.isDirect()) {
                return ByteBuffer.allocateDirect((int) (buffer.capacity() * 1.5));
            }
            return ByteBuffer.allocate((int) (buffer.capacity() * 1.5));
        }
        return buffer;
    }

    public static void putTime(Instant time, ByteBuffer buffer) {
        buffer.putLong(time.getEpochSecond());
        buffer.putInt(time.getNano());
    }

    public static Instant getTime(ByteBuffer buffer) {
        return Instant.ofEpochSecond(buffer.getLong(), buffer.getInt());
    }

    public static boolean requireTrue(boolean bool, String errMsg) {
        if (!bool)
            throw new RuntimeException(errMsg);
        return true;
    }

    public static boolean requireFalse(boolean bool, String errMsg) {
        if (bool)
            throw new RuntimeException(errMsg);
        return false;
    }

    public static boolean requireEquals(int a, int b, String errMsg) {
        if (a != b)
            throw new RuntimeException(errMsg);
        return true;
    }

    public static boolean requireEquals(long a, long b, String errMsg) {
        if (a != b)
            throw new RuntimeException(errMsg);
        return true;
    }

    public static boolean requireNotEquals(long a, long b, String errMsg) {
        if (a == b)
            throw new RuntimeException(errMsg);
        return true;
    }

    public static boolean requireNotEquals(int a, int b, String errMsg) {
        if (a == b)
            throw new RuntimeException(errMsg);
        return true;
    }

    public static String fileSeparatorForSplit = Pattern.quote(File.separator);
}
