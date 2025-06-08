package org.example.intervalsAlgo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LinearIntervalSearch implements IntervalAlgo {
    private final Set<Interval> intervals = new HashSet<>();

    @Override
    public List<Interval> getIntervals(byte[] key) {
        return intervals.parallelStream().filter(each -> each.contains(key)).collect(Collectors.toList());
    }

    @Override
    public void insert(Interval interval) {
        if (!intervals.add(interval)) {
            throw new RuntimeException("Same interval added");
        }
    }
}
