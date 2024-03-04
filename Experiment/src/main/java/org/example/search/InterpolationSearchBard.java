package org.example.search;

public class InterpolationSearchBard implements Search{
    public int search1(int[] arr, int target) {
        int low = 0;
        int high = arr.length - 1;

        while (low <= high) {
            // Calculate the interpolation position using linear interpolation
            double position = low + (double) (target - arr[low]) / (arr[high] - arr[low]) * (high - low);

            // Check for invalid interpolation (target outside range)
            if (position < low || position > high) {
                return -1;
            }

            int index = (int) Math.round(position);

            // If target is found at the interpolated index
            if (arr[index] == target) {
                return index;
            } else if (arr[index] < target) {
                // Search right half
                low = index + 1;
            } else {
                // Search left half
                high = index - 1;
            }
        }

        return -1; // Target not found
    }

    @Override
    public int search(int[] arr, int target) {

        int low = 0;
        int high = arr.length - 1;

        while (low <= high && arr[low] <= target && arr[high] >= target) {
            float fx = 1.0f * (target - arr[low]) / (arr[high] - arr[low]);
            int mid = (int) (low + fx * (high - low));
            if (arr[mid] == target) {
                return mid;
            } else if (arr[mid] > target) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return -1;
    }
}
