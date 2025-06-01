package org.g2n.atomdb.util;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class BytesConverter {

    public static byte[] bytes(String value) {
        requireNonNull(value);
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] bytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return result;
    }

    public static byte[] toByteArray(int value) {
        return new byte[] {
                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
        };
    }

    public long fromBytes(byte[] arr) {
        if (arr.length != Long.BYTES) {
            throw new ArrayIndexOutOfBoundsException("required length " + Long.BYTES);
        }
        return fromBytesToLong(arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],
                arr[6],arr[7]);
    }


    public static byte[] longToByteArray(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return result;
    }

    public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
        };
    }

    public static int bytesToInt(byte[] arr) {
        return arr[0] << 24 | (arr[1] & 0xFF) << 16 | (arr[2] & 0xFF) << 8 | (arr[3] & 0xFF);
    }

    public static long bytesToLong(byte[] arr) {
        return fromBytesToLong(arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],
                arr[6],arr[7]);
    }

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
