package org.example;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        int[] ints = IntStream.range(0, 100000).toArray();
        int iterations = (int) Math.ceil(Math.log(ints.length)/Math.log(2));
        var frequentlyUsedValues = new TreeMap<Integer, Integer>();
        getFrequentlyUsedValues(ints, 0, ints.length - 1, frequentlyUsedValues, (int) Math.ceil(iterations * 0.75));
        System.out.println(frequentlyUsedValues.keySet());
        System.out.println("Size: " + frequentlyUsedValues.keySet().size());
        int usingCount = 0;
        int loopCount = 0;
        var frquencyCountMap = new TreeMap<Integer, Integer>();
        for (int i = 0; i < ints.length; i++) {
            var counts =  search(ints, i, frequentlyUsedValues, frquencyCountMap);
            loopCount += counts[0];
            usingCount += counts[1];
        }
        System.out.println("map used=" + usingCount);
        System.out.println("total bs iteration count=" + loopCount);
        System.out.println("total iteration by math" + (iterations * ints.length));
        for (Map.Entry<Integer, Integer> entry : frquencyCountMap.entrySet()) {
            if (entry.getValue() <= 50) {
                continue;
            }
            System.out.println("Position: "+ entry.getKey() + "was used " + entry.getValue() + " times");
        }
    }

    private static int[] search(int[] arr, int key, Map<Integer, Integer> frequentlyUsedValues, Map<Integer, Integer> frequentCountMap) {
        int l = 0, h = arr.length - 1;
        int usingCount = 0;
        int loopCount = 0;
        while (l <= h) {
            loopCount++;
            int mid = (l + h) >>> 1;
            frequentCountMap.put(mid, frequentCountMap.getOrDefault(mid, 0) + 1);
            int foundKey;
            if (frequentlyUsedValues.containsKey(mid)) {
                usingCount++;
                foundKey = frequentlyUsedValues.get(mid);
            } else {
                foundKey = arr[mid];
            }
            int compare = Integer.compare(foundKey, key);
            if (compare < 0)
                l = mid + 1;
            else if (compare > 0)
                h = mid - 1;
            else {
                break;
//                return mid;
            }
        }
        return new int[] {loopCount, usingCount};
    }

    private static void getFrequentlyUsedValues (int[] arr, int low, int high, Map<Integer, Integer> frequentlyUsedMids, int iterations) {
        if (iterations <= 0) return;

        int mid = (low + high) >>> 1;
        if (mid < 0 || arr.length <= mid) return;
        frequentlyUsedMids.put(mid, arr[mid]);
        getFrequentlyUsedValues(arr, low, mid - 1, frequentlyUsedMids, iterations - 1);
        getFrequentlyUsedValues(arr, mid + 1, high, frequentlyUsedMids, iterations - 1);
    }
}