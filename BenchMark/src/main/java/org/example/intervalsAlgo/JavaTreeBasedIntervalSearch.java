package org.example.intervalsAlgo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

public class JavaTreeBasedIntervalSearch implements IntervalAlgo{
    TreeMap<byte[], List<Interval>> intervalMap = new TreeMap<>(Arrays::compare);
    @Override
    public List<Interval> getIntervals(byte[] key) {
        List<Interval> result = new ArrayList<>();

        // Get all starts <= x
        for (var entry : intervalMap.headMap(increment(key)).entrySet()) {
            for (Interval interval : entry.getValue()) {
                if (interval.contains(key)) {
                    result.add(interval);
                }
            }
        }
        return result;
    }

    @Override
    public void insert(Interval interval) {
        intervalMap.computeIfAbsent(interval.low, _ -> new ArrayList<>())
                .add(interval);
    }

    public byte[] incrementOriginal(byte[] arr) {
        if (arr == null) {
            throw new IllegalArgumentException("Input array cannot be null.");
        }

        // Create a copy to avoid modifying the original array.
        byte[] result = arr.clone();

        // Iterate from the last byte (least significant) to the first.
        for (int i = result.length - 1; i >= 0; i--) {
            // Increment the current byte.
            result[i]++;

            // If the byte did not wrap around from 0xFF to 0x00, we are done.
            // The ++ operator on a byte 0xFF results in 0x00, so we check if it's non-zero.
            if (result[i] != 0) {
                return result; // No carry, so we can exit.
            }

            // If result[i] is 0, it means there was a carry, and the loop continues
            // to the next most significant byte.
        }

        // If the loop completes, it means the entire array was 0xFF's and has overflowed.
        // We need to return a new array that is one byte longer.
        byte[] overflowResult = new byte[result.length + 1];
        overflowResult[0] = 0x01; // The most significant byte is 1.
        // The rest of the bytes are already 0 by default in Java.

        return overflowResult;
    }

    public byte[] increment(byte[] arr) {
        if (arr == null) {
            throw new IllegalArgumentException("Input array cannot be null.");
        }

        byte[] result = arr.clone();
        if (result[result.length - 1] < Byte.MAX_VALUE) {
            result[result.length - 1] = (byte) (result[result.length - 1] + (byte)1);
            return result;
        }

        byte[] overflowResult = new byte[result.length + 1];
        System.arraycopy(result, 0, overflowResult, 0, result.length);
        overflowResult[overflowResult.length -1] = 0x01;

        return overflowResult;
    }
}
