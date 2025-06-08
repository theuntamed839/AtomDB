package org.example.intervalsAlgo;

import java.util.Arrays;

public class Interval implements Comparable<Interval> {
    final byte[] low, high;
    final String name;

    public Interval(byte[] low, byte[] high, String name) {
        if (Arrays.compare(low, high) > 0) {
            throw new RuntimeException("low can't be bigger then high");
        }
        this.low = low;
        this.high = high;
        this.name = name;
    }

    public boolean contains(byte[] key) {
        return Arrays.compare(low, key) <= 0 &&
                Arrays.compare(high, key) >= 0;
    }

    public boolean intersects(Interval other) {
        return (Arrays.compare(low, other.high) <= 0 &&
                Arrays.compare(high, other.low) >= 0);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int compareTo(Interval interval) {
        int lowCompare = Arrays.compare(low, interval.low);
        if (lowCompare != 0) {
            return lowCompare;
        }
        return Arrays.compare(high, interval.high);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Interval interval)) return false;

        return Arrays.equals(low, interval.low) && Arrays.equals(high, interval.high);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(low);
        result = 31 * result + Arrays.hashCode(high);
        return result;
    }
}