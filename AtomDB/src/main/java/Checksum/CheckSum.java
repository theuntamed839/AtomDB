package Checksum;

import org.xerial.snappy.PureJavaCrc32C;

import java.time.Instant;

import static util.BytesConverter.bytes;

// todo synchronize
public class CheckSum {
    private static PureJavaCrc32C pureJavaCrc32C = new PureJavaCrc32C();
//    private static Object obj = new Object();

    private static long computeCheckSum(byte[] arr) {
        pureJavaCrc32C.reset();
        pureJavaCrc32C.update(arr);
        return pureJavaCrc32C.getValue();
    }

    private static long computeCheckSum(byte arr) {
        pureJavaCrc32C.reset();
        pureJavaCrc32C.update(arr);
        return pureJavaCrc32C.getValue();
    }

    public static long logBlock(Instant time, byte[] operation,
                                byte[] key, byte[] value) {
        pureJavaCrc32C.reset();

        //Instant time storage
        pureJavaCrc32C.update(bytes(time.getEpochSecond()));
        pureJavaCrc32C.update(time.getNano());

        pureJavaCrc32C.update(operation);
        pureJavaCrc32C.update(key);
        pureJavaCrc32C.update(value);

        return pureJavaCrc32C.getValue();
    }

    public static long compute(byte[] arr) {
        pureJavaCrc32C.reset();
        pureJavaCrc32C.update(arr);
        return pureJavaCrc32C.getValue();
    }

    public static long compute(byte[] key, byte[] value) {
        pureJavaCrc32C.reset();
        pureJavaCrc32C.update(key);
        pureJavaCrc32C.update(value);
        return pureJavaCrc32C.getValue();
    }
}
