package org.example.search;

public class ExponentialSearchChatGPT implements Search {

    @Override
    public int search(int[] arr, int target) {
        int n = arr.length;
        if (arr[0] == target) {
            return 0; // Found at the first element
        }

        int i = 1;
        while (i < n && arr[i] <= target) {
            i *= 2; // Double the index
        }

        // Perform binary search within the found range
        return binarySearch(arr, target, i / 2, Math.min(i, n - 1));
    }

    private static int binarySearch(int[] arr, int target, int low, int high) {
        while (low <= high) {
            int mid = low + (high - low) / 2;
            if (arr[mid] == target) {
                return mid;
            } else if (arr[mid] < target) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return -1; // Element not found
    }
}
