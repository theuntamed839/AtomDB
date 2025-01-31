package org.g2n.atomdb.util;

public class MaxMinAvg {
    public long max;
    public long min;
    public double avg;
    public int times;

    public MaxMinAvg(long value) {
        this.max = value;
        this.min = value;
        this.avg = value;
    }

    public MaxMinAvg() {
        this.max = Integer.MIN_VALUE;
        this.min = Integer.MAX_VALUE;
        this.avg = 0;
    }

    public MaxMinAvg add(long time) {
        this.max = Math.max(max, time);
        this.min = Math.min(min, time);
        this.avg = avg == 0 ? time : (avg + time)/2.0;
        this.times++;
        return this;
    }
}