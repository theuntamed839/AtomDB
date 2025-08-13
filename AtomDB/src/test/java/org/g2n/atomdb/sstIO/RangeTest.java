package org.g2n.atomdb.sstIO;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RangeTest {
    private static byte[] b(String s) {
        return s.getBytes();
    }

    @Test
    void testInRange_middle() {
        Range range = new Range(b("a"), b("z"));
        assertTrue(range.inRange(b("m")));
    }

    @Test
    void testInRange_lowerBound() {
        Range range = new Range(b("a"), b("z"));
        assertTrue(range.inRange(b("a")));
    }

    @Test
    void testInRange_upperBound() {
        Range range = new Range(b("a"), b("z"));
        assertTrue(range.inRange(b("z")));
    }

    @Test
    void testInRange_belowRange() {
        Range range = new Range(b("a"), b("z"));
        assertFalse(range.inRange(b("A")));
    }

    @Test
    void testInRange_aboveRange() {
        Range range = new Range(b("a"), b("z"));
        assertFalse(range.inRange(b("zz")));
    }

    @Test
    void testOverlapsWith_fullOverlap() {
        Range r1 = new Range(b("a"), b("z"));
        Range r2 = new Range(b("c"), b("y"));
        assertTrue(r1.overlapsWith(r2));
        assertTrue(r2.overlapsWith(r1));
    }

    @Test
    void testOverlapsWith_partialOverlap() {
        Range r1 = new Range(b("a"), b("m"));
        Range r2 = new Range(b("k"), b("z"));
        assertTrue(r1.overlapsWith(r2));
        assertTrue(r2.overlapsWith(r1));
    }

    @Test
    void testOverlapsWith_noOverlap() {
        Range r1 = new Range(b("a"), b("f"));
        Range r2 = new Range(b("g"), b("z"));
        assertFalse(r1.overlapsWith(r2));
        assertFalse(r2.overlapsWith(r1));
    }

    @Test
    void testContains_true() {
        Range r1 = new Range(b("a"), b("z"));
        Range r2 = new Range(b("b"), b("y"));
        assertTrue(r1.contains(r2));
    }

    @Test
    void testContains_false_smallerStart() {
        Range r1 = new Range(b("b"), b("z"));
        Range r2 = new Range(b("a"), b("y"));
        assertFalse(r1.contains(r2));
    }

    @Test
    void testContains_false_largerEnd() {
        Range r1 = new Range(b("a"), b("y"));
        Range r2 = new Range(b("b"), b("z"));
        assertFalse(r1.contains(r2));
    }

    @Test
    void testContains_exactMatch() {
        Range r1 = new Range(b("a"), b("z"));
        Range r2 = new Range(b("a"), b("z"));
        assertTrue(r1.contains(r2));
    }
}