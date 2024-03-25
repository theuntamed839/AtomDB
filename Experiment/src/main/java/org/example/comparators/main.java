package org.example.comparators;

import com.google.common.primitives.UnsignedBytes;

import java.util.*;

public class main {
    static Comparator<byte[]> atomDBCurrentComparator = (left, right) -> {
        int minLength = Math.min(left.length, right.length);
        for (int i = 0; i < minLength; i++) {
            int result = left[i] - right[i];
            if (result != 0) {
                return result;
            }
        }
        return left.length - right.length;
    };

    static Comparator<byte[]> javasComparator = Arrays::compare;
    static Comparator<byte[]> guavaComparator = UnsignedBytes.lexicographicalComparator();
    public static void main(String[] args) {
        Random rand = new Random();
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < 1000_0000; i++) {
            byte[] array = new byte[1024];
            rand.nextBytes(array);
            list.add(array);
        }
        benchMark(javasComparator, list, "java");
        benchMark(atomDBCurrentComparator, list, "atom");
        benchMark(guavaComparator, list, "guava");

    }

    private static void benchMark(Comparator<byte[]> comparator, List<byte[]> arr,String name) {
        long start, end;
        start = System.nanoTime();
        for (int i = 0, j = arr.size() - 1; i < arr.size(); i++, j--) {
            comparator.compare(arr.get(i), arr.get(j));
        }
        end = System.nanoTime();
        System.out.println(name + " took "+(end-start));
    }

}
