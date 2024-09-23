package org.example.guava;

import java.util.Arrays;
import java.util.Comparator;

class MyByte  implements Comparable<MyByte>, Comparator {

    byte[] arr;

    public MyByte(byte[] arr) {
        this.arr = arr;
    }

    public int compareTo(byte[] bytes) {
        return Arrays.compare(arr, bytes);
    }


    @Override
    public int compare(Object o, Object t1) {
        return Arrays.compare((byte[]) o, (byte[]) t1);
    }

    @Override
    public int compareTo(MyByte myByte) {

        return Arrays.compare(arr, myByte.arr);
    }
}
