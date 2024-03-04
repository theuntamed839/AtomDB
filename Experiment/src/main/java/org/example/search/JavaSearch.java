package org.example.search;

import java.util.Arrays;

public class JavaSearch implements Search{
    @Override
    public int search(int[] arr, int target) {
        return Arrays.binarySearch(arr, target);
    }
}
