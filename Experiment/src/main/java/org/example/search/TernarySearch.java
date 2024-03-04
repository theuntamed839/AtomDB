package org.example.search;

public class TernarySearch implements Search{

    @Override
    public int search(int[] arr, int target) {
        return ternarySearch(arr, target, 0, arr.length - 1);
    }

    private static int ternarySearch(int[] arr, int target, int low, int high) {
        if (low <= high) {
            int mid1 = low + (high - low) / 3;
            int mid2 = high - (high - low) / 3;

            if (arr[mid1] == target) {
                return mid1;
            }
            if (arr[mid2] == target) {
                return mid2;
            }

            if (target < arr[mid1]) {
                return ternarySearch(arr, target, low, mid1 - 1);
            } else if (target > arr[mid2]) {
                return ternarySearch(arr, target, mid2 + 1, high);
            } else {
                return ternarySearch(arr, target, mid1 + 1, mid2 - 1);
            }
        }
        return -1; // Element not found
    }

}
