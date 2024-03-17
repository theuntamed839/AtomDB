package util;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class BytesConverter {

    public static byte[] bytes(String value) {
        requireNonNull(value);
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // google guava
    public static byte[] bytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return result;
    }

    public static long toLong(byte[] bytes, int inclusive, int exclusive) {
        requireNonNull(bytes);
        if (bytes.length < exclusive) {
            throw new ArrayIndexOutOfBoundsException("provided exclusive="+exclusive+" for an array of length="+bytes.length);
        }
        if (Math.abs(exclusive - inclusive) != Long.BYTES) {
            throw new ArrayIndexOutOfBoundsException("provided bound not fit for long");
        }
        return fromBytesToLong(bytes[inclusive],
                bytes[inclusive + 1],
                bytes[inclusive + 2],
                bytes[inclusive + 3],
                bytes[inclusive + 4],
                bytes[inclusive + 5],
                bytes[inclusive + 6],
                bytes[inclusive + 7]);
    }

    public static int toInt(byte[] bytes, int inclusive, int exclusive) {
        requireNonNull(bytes);
        if (bytes.length < exclusive) {
            throw new ArrayIndexOutOfBoundsException("provided exclusive="+exclusive+" for an array of length="+bytes.length);
        }
        if (Math.abs(exclusive - inclusive) != Integer.BYTES) {
            throw new ArrayIndexOutOfBoundsException("provided bound not fit for Integer, inclusive="+inclusive+", exclusive="+exclusive);
        }
        return fromBytesToInt(bytes[inclusive],
                bytes[inclusive + 1],
                bytes[inclusive + 2],
                bytes[inclusive + 3]);
    }

    // google guava
    public static byte[] toByteArray(int value) {
        return new byte[] {
                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
        };
    }

    // google guava
    public static int fromBytesToInt(byte b1, byte b2, byte b3, byte b4) {
        return b1 << 24 | (b2 & 0xFF) << 16 | (b3 & 0xFF) << 8 | (b4 & 0xFF);
    }

    public long fromBytes(byte[] arr) {
        if (arr.length != Long.BYTES) {
            throw new ArrayIndexOutOfBoundsException("required length " + Long.BYTES);
        }
        return fromBytesToLong(arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],
                arr[6],arr[7]);
    }

    // google guava
    //https://github.com/google/guava/blob/d5fbccac90aba8501c633e896ea67e2b0bfb426d/guava/src/com/google/common/primitives/Longs.java#L284
    private static long fromBytesToLong(
            byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
        return (b1 & 0xFFL) << 56
                | (b2 & 0xFFL) << 48
                | (b3 & 0xFFL) << 40
                | (b4 & 0xFFL) << 32
                | (b5 & 0xFFL) << 24
                | (b6 & 0xFFL) << 16
                | (b7 & 0xFFL) << 8
                | (b8 & 0xFFL);
    }
}
