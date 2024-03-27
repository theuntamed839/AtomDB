package org.example.longestCommonPrefix;

import java.util.*;

public class LongestCommonPrefix {

    public static byte[] findCommonPrefix(List<byte[]> byteArrays) {
        if (byteArrays.isEmpty()) {
            return new byte[0];
        }

        var shortest = byteArrays.getFirst();
        for (byte[] byteArray : byteArrays) {
            if (shortest.length > byteArray.length) {
                shortest = byteArray;
            }
        }

        for (int i = 0; i < shortest.length; i++) {
            byte currentByte = shortest[i];
            for (byte[] otherArray : byteArrays) {
                if (i >= otherArray.length || otherArray[i] != currentByte) {
                    return Arrays.copyOfRange(shortest, 0, i);
                }
            }
        }

        // All bytes matched, return the entire shortest array
        return shortest;
    }

    public static byte[] findCommonPrefixOptimized(List<byte[]> byteArrays) {
        // this considers that the array is already sorted. and we just need to check for first and last for the prefix.
//        if (byteArrays == null || byteArrays.isEmpty()) {
//            throw new RuntimeException("Byte array provided is null or empty, byteArray="+byteArrays);
//        }

        var first = byteArrays.getFirst();
        var last = byteArrays.get(1);
        for (int i = 0; i < first.length; i++) {
            if (first[i] != last[i]) {
                return Arrays.copyOfRange(first, 0, i);
            }
        }

        // All bytes matched, return the entire shortest array
        return first;
    }

    public static void main(String[] args) {
        long start, end;
        Random random = new Random();
        List<byte[]> byteArrays = new ArrayList<>();
        for (int i = 0; i <= 100000; i++) {
            var by = new byte[500];
            random.nextBytes(by);
            byteArrays.add(by);
        }
        start = System.nanoTime();
        for (int i = 10; i < byteArrays.size(); i+= 10) {
            prefix(byteArrays.subList(i - 10, i));
        }
        byteArrays.sort(Arrays::compare);
        end = System.nanoTime();
        System.out.println("nano"+(end - start));
    }

    private static void prefix(List<byte[]> byteArrays) {
        byte[] commonPrefix = findCommonPrefix(byteArrays);
//        byte[] commonPrefix = findCommonPrefixOptimized(byteArrays);
//        if (commonPrefix.length == 0 || commonPrefix.length == 1) {
//            return;
//        }
//        System.out.println("prefix" + Arrays.toString(commonPrefix));
//        for (byte[] byteArray : byteArrays) {
//            System.out.println(Arrays.toString(byteArray));
//        }
    }
}