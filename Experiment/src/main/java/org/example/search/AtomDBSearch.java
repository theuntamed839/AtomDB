package org.example.search;

public class AtomDBSearch implements Search{
    @Override
    public int search(int[] arr, int key) {
        int l = 0, h = arr.length -1;
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
