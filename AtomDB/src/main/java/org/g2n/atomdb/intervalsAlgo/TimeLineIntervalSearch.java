package org.g2n.atomdb.intervalsAlgo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class TimeLineIntervalSearch implements IntervalAlgo{
    private final List<List<Interval>> set = new ArrayList<>();

    @Override
    public List<Interval> getIntervals(byte[] key) {
        List<Interval> result = new ArrayList<>();
        for (List<Interval> intervals : set) {
            if (intervals.isEmpty()) continue;
            if (Arrays.compare(intervals.getFirst().low, key) > 0) {
                continue; // Sorted by start, so we can stop early
            }
            if (Arrays.compare(intervals.getLast().high, key) < 0) {
                continue; // Sorted by start, so we can stop early
            }
            Interval overlapping = findOverlapping(intervals, key);

            if (overlapping != null) {
                result.add(overlapping);
            }
        }
        return result;
    }

    private Interval findOverlapping(List<Interval> intervals, byte[] target) {
        int low = 0, high = intervals.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;

            Interval midInterval = intervals.get(mid);
            if (midInterval.contains(target)) {
                return midInterval;
            } else if (Arrays.compare(target, midInterval.low) < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    @Override
    public void insert(Interval interval) {
        for (List<Interval> intervals : set) {
            boolean intersects = false;
            for (Interval existing : intervals) {
                if (existing.intersects(interval)) {
                    if (existing.equals(interval)) {
                        return; // already existing one.
                    }
                    intersects = true;
                    break;
                }
            }

            if (!intersects) {
                intervals.add(interval);
                intervals.sort((a1, a2) -> Arrays.compare(a1.low, a2.low)); // Optional if insert always keeps sorted
                return;
            }
        }
        List<Interval> newList = new ArrayList<>();
        newList.add(interval);
        set.add(newList);
    }

    @Override
    public void remove(Interval interval) {
        set.stream().filter(each -> each.contains(interval)).findFirst().get().remove(interval);
    }
}
