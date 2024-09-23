package org.example.search;

public class ExponentialSearch implements Search{

    @Override
    public int search(int[] A, int key) {
        // lower and upper bound for binary search
        int lower_bound = 0;
        int upper_bound = 1;

        // calculate lower and upper bound
        while (A[upper_bound] < key) {
            lower_bound = upper_bound;
            upper_bound = upper_bound * 2;
        }

        return atomDBBinarySearch(A, key, lower_bound, upper_bound);
    }

    private int atomDBBinarySearch(int[] arr, int key, int l, int h) {
        while(l <= h) {
            int mid = (l + h) >>> 1;
            int foundKey = arr[mid];
            int compare = Integer.compare(foundKey, key);
            if (compare < 0)
                l = mid + 1;
            else if (compare > 0)
                h = mid - 1;
            else
                return mid;
        }
        return -1;
    }
}