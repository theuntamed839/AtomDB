package org.g2n.atomdb.Checksum;

import org.xerial.snappy.PureJavaCrc32C;


// todo synchronize
public class CheckSumStatic {
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
