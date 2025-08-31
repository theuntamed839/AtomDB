package io.github.theuntamed839.datastore4j.sstIO;

import org.junit.jupiter.api.Test;

import static io.github.theuntamed839.datastore4j.util.BytesConverter.bytes;
import static org.junit.jupiter.api.Assertions.*;

class RangeTest {

    @Test
    void testInRange_middle() {
        Range range = new Range(bytes("a"), bytes("z"));
        assertTrue(range.inRange(bytes("m")));
    }

    @Test
    void testInRange_lowerBound() {
        Range range = new Range(bytes("a"), bytes("z"));
        assertTrue(range.inRange(bytes("a")));
    }

    @Test
    void testInRange_upperBound() {
        Range range = new Range(bytes("a"), bytes("z"));
        assertTrue(range.inRange(bytes("z")));
    }

    @Test
    void testInRange_belowRange() {
        Range range = new Range(bytes("a"), bytes("z"));
        assertFalse(range.inRange(bytes("A")));
    }

    @Test
    void testInRange_aboveRange() {
        Range range = new Range(bytes("a"), bytes("z"));
        assertFalse(range.inRange(bytes("zz")));
    }

    @Test
    void testOverlapsWith_fullOverlap() {
        Range r1 = new Range(bytes("a"), bytes("z"));
        Range r2 = new Range(bytes("c"), bytes("y"));
        assertTrue(r1.overlapsWith(r2));
        assertTrue(r2.overlapsWith(r1));
    }

    @Test
    void testOverlapsWith_partialOverlap() {
        Range r1 = new Range(bytes("a"), bytes("m"));
        Range r2 = new Range(bytes("k"), bytes("z"));
        assertTrue(r1.overlapsWith(r2));
        assertTrue(r2.overlapsWith(r1));
    }

    @Test
    void testOverlapsWith_noOverlap() {
        Range r1 = new Range(bytes("a"), bytes("f"));
        Range r2 = new Range(bytes("g"), bytes("z"));
        assertFalse(r1.overlapsWith(r2));
        assertFalse(r2.overlapsWith(r1));
    }

    @Test
    void testContains_true() {
        Range r1 = new Range(bytes("a"), bytes("z"));
        Range r2 = new Range(bytes("b"), bytes("y"));
        assertTrue(r1.contains(r2));
    }

    @Test
    void testContains_false_smallerStart() {
        Range r1 = new Range(bytes("b"), bytes("z"));
        Range r2 = new Range(bytes("a"), bytes("y"));
        assertFalse(r1.contains(r2));
    }

    @Test
    void testContains_false_largerEnd() {
        Range r1 = new Range(bytes("a"), bytes("y"));
        Range r2 = new Range(bytes("b"), bytes("z"));
        assertFalse(r1.contains(r2));
    }

    @Test
    void testContains_exactMatch() {
        Range r1 = new Range(bytes("a"), bytes("z"));
        Range r2 = new Range(bytes("a"), bytes("z"));
        assertTrue(r1.contains(r2));
    }
}