package io.github.theuntamed839.datastore4j.util;

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

    public static long bytesToLong(byte[] arr) {
        return (arr[0] & 0xFFL) << 56
                | (arr[1] & 0xFFL) << 48
                | (arr[2] & 0xFFL) << 40
                | (arr[3] & 0xFFL) << 32
                | (arr[4] & 0xFFL) << 24
                | (arr[5] & 0xFFL) << 16
                | (arr[6] & 0xFFL) << 8
                | (arr[7] & 0xFFL);
    }
}
