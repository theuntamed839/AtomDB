package org.g2n.atomdb.intervalsAlgo;

import org.g2n.atomdb.table.SSTInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IntervalTree implements IntervalAlgo {
    private IntervalNode root;

    public void insert(Interval interval) {
        root = insert(root, interval);
    }

    private IntervalNode insert(IntervalNode node, Interval interval) {
        if (node == null) return new IntervalNode(interval);

        if (Arrays.compare(interval.low, node.interval.low) < 0 ){
            node.left = insert(node.left, interval);
        } else {
            node.right = insert(node.right, interval);
        }

        if (Arrays.compare(node.max, interval.high) < 0) {
            node.max = interval.high;
        }
        return node;
    }


    private void search(IntervalNode node, byte[] key, List<SSTInfo> result) {
        if (node == null) return;

        if (node.interval.contains(key)) {
            result.add(node.interval.name);
        }

        if (node.left != null && Arrays.compare(key, node.left.max) <= 0) {
            search(node.left, key, result);
        }

        search(node.right, key, result);
    }

    private void searchIntervals(IntervalNode node, byte[] key, List<Interval> result) {
        if (node == null) return;

        if (node.interval.contains(key)) {
            result.add(node.interval);
        }

        if (node.left != null && Arrays.compare(key, node.left.max) <= 0) {
            searchIntervals(node.left, key, result);
        }

        searchIntervals(node.right, key, result);
    }

    private void searchOverlap(IntervalNode node, Interval target, List<SSTInfo> result) {
        if (node == null) return;

        if (node.interval.intersects(target)) {
            result.add(node.interval.name);
        }

        if (node.left != null && Arrays.compare(node.left.max, target.low) >= 0) {
            searchOverlap(node.left, target, result);
        }

        searchOverlap(node.right, target, result);
    }

    public List<SSTInfo> searchContaining(byte[] key) {
        List<SSTInfo> result = new ArrayList<>();
        search(root, key, result);
        return result;
    }

    @Override
    public void remove(Interval interval) {
        root = delete(root, interval);
    }

    private IntervalNode delete(IntervalNode node, Interval interval) {
        if (node == null) return null;

        int cmp = Arrays.compare(interval.low, node.interval.low);

        if (cmp < 0) {
            node.left = delete(node.left, interval);
        } else if (cmp > 0) {
            node.right = delete(node.right, interval);
        } else if (node.interval.equals(interval)) {
            // Node to delete found
            if (node.left == null) return node.right;
            if (node.right == null) return node.left;

            // Two children: Replace with in-order successor
            IntervalNode min = getMin(node.right);
            node.interval = min.interval;
            node.right = delete(node.right, min.interval);
        } else {
            // Same low but not exactly equal, go right
            node.right = delete(node.right, interval);
        }

        // Update max
        node.max = node.interval.high;
        if (node.left != null && Arrays.compare(node.left.max, node.max) > 0) {
            node.max = node.left.max;
        }
        if (node.right != null && Arrays.compare(node.right.max, node.max) > 0) {
            node.max = node.right.max;
        }

        return node;
    }

    private IntervalNode getMin(IntervalNode node) {
        while (node.left != null) {
            node = node.left;
        }
        return node;
    }

    @Override
    public List<Interval> getIntervals(byte[] key) {
        List<Interval> result = new ArrayList<>();
        searchIntervals(root, key, result);
        return result;
    }
}

class IntervalNode {
    Interval interval;
    byte[] max; // max high value in this subtree
    IntervalNode left, right;

    public IntervalNode(Interval interval) {
        this.interval = interval;
        this.max = interval.high;
    }
}

