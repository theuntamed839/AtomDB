package org.example.intervalsAlgo;

import java.util.*;

public class TimeLineIntervalSearchJavaBinarySearch implements IntervalAlgo{
    private final List<List<Interval>> set = new ArrayList<>();

    @Override
    public List<Interval> getIntervals(byte[] key) {
        var list = new ArrayList<Interval>();
        Interval toSearch = new Interval(key, key, "");
        for (List<Interval> intervals : set) {
            if (Arrays.compare(intervals.getFirst().low, key) > 0) {
                continue; // Sorted by start, so we can stop early
            }
            if (Arrays.compare(intervals.getLast().high, key) < 0) {
                continue; // Sorted by start, so we can stop early
            }
            int index = Collections.binarySearch(intervals, toSearch, (a1, a2) -> {
                if (a1.intersects(a2)) {
                    return 0;
                }
                return a1.compareTo(a2);
            });
            if (index >= 0) {
                list.add(intervals.get(index));
            }
        }
        return list;
    }

    @Override
    public void insert(Interval interval) {
        for (List<Interval> intervals : set) {
            int index = Collections.binarySearch(intervals, interval, (a1, a2) -> {
                if (a1.intersects(a2)) {
                    return 0;
                }
                return a1.compareTo(a2);
            });

            if (index < 0) {
                intervals.add(~index, interval);
                intervals.sort((a1, a2) -> Arrays.compare(a1.low, a2.low));
                return;
            } else {
                if (intervals.get(index).equals(interval)) {
                    return; // we already have this interval
                }
            }
        }

        var tree = new ArrayList<Interval>();
        tree.add(interval);
        set.add(tree);
    }
}
