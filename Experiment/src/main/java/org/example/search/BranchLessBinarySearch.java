package org.example.search;

public class BranchLessBinarySearch implements Search {

    @Override
    public int search(int[] arr, int target) {
        int low = 0;
        int high = arr.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1; // Calculate midpoint without branching

            // Use bitwise operations to emulate branching
            int cmp = (arr[mid] - target) >>> 31;
            low += (cmp & 1) * (mid - low + 1);
            high -= ((cmp ^ 1) & 1) * (high - mid + 1);

            if (arr[mid] == target) {
                return mid;
            }
        }

        return -1; // Element not found
    }
}
