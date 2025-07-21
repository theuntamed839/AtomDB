package org.g2n.atomdb.intervalsAlgo;

import org.g2n.atomdb.sstIO.Range;

import java.util.*;

class idk {
    private final ArrayList<Interval> intervals;
    private Range range;

    public idk(Interval interval) {
        this.intervals = new ArrayList<>();
        this.range = interval.get().getSstKeyRange();
        this.intervals.add(interval);
    }

    public boolean insertIfMutuallyExclusive(Interval interval) {
        if (!range.overLapping(interval.get().getSstKeyRange())) {
            insert(interval);
            return true;
        }

        if (getIntervalContaining(interval.low) == null && getIntervalContaining(interval.high) == null) {
            insert(interval);
            return true;
        }

        return false;
    }

    private void insert(Interval interval) {
        intervals.add(interval);
        intervals.sort((a1, a2) -> Arrays.compare(a1.low, a2.low));
        this.range = new Range(
                intervals.getFirst().low,
                intervals.stream().max((a1, a2) -> Arrays.compare(a1.high, a2.high)).get().high
        );
    }

    public boolean remove(Interval interval) {
        if (intervals.remove(interval)) {
            if (intervals.isEmpty()) {
                this.range = null;
            } else {
                this.range = new Range(
                        intervals.getFirst().low,
                        intervals.stream().max((a1, a2) -> Arrays.compare(a1.high, a2.high)).get().high
                );
            }
            return true;
        }
        return false;
    }

    public Interval findTarget(byte[] target) {
        if (this.range.inRange(target)) {
            return getIntervalContaining(target);
        }
        return null;
    }

    private Interval getIntervalContaining(byte[] target) {
        if (intervals.isEmpty()) {
            throw new RuntimeException("Empty intervals");
        }

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

    public boolean isEmpty() {
        return intervals.isEmpty();
    }
}

public class TimeLineIntervalOptimizedSearch implements IntervalAlgo{
    private final List<idk> set = new ArrayList<>();//Collections.synchronizedList(new ArrayList<>());

    @Override
    public List<Interval> getIntervals(byte[] key) {
        List<Interval> result = new ArrayList<>();
        for (idk intervals : set) {
            Interval overlapping = intervals.findTarget(key);
            if (overlapping != null) {
                result.add(overlapping);
            }
        }
        return result;
    }

    @Override
    public void insert(Interval interval) {
        for (idk intervals : set) {
            if (intervals.insertIfMutuallyExclusive(interval)) {
                return;
            }
        }

        set.add(new idk(interval));
    }

    @Override
    public void remove(Interval interval) {
        List<idk> toDelete = new ArrayList<>();
        for (idk intervals : set) {
            intervals.remove(interval);
            if (intervals.isEmpty()) {
                toDelete.add(intervals);
            }
        }
        set.removeAll(toDelete);
    }
}
