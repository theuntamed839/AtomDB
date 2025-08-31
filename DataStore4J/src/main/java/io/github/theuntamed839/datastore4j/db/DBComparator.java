package io.github.theuntamed839.datastore4j.db;

import java.util.Arrays;
import java.util.Comparator;

public class DBComparator {
    public static final Comparator<byte[]> byteArrayComparator = Arrays::compare;

    private static int compare(byte[] left, byte[] right) {
        return Arrays.compare(left, right);
//        int minLength = Math.min(left.length, right.length);
//        for (int i = 0; i < minLength; i++) {
//            int result = left[i] - right[i];
//            if (result != 0) {
//                return result;
//            }
//        }
//        return left.length - right.length;
    }
}
