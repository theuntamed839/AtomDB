package org.example.intervalsAlgo;

import java.util.List;

public interface IntervalAlgo {
    public List<Interval> getIntervals(byte[] key);
    public void insert(Interval interval);
}
