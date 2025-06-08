package org.example.intervalsAlgo;

import java.util.*;

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


    private void search(IntervalNode node, byte[] key, List<String> result) {
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

    private void searchOverlap(IntervalNode node, Interval target, List<String> result) {
        if (node == null) return;

        if (node.interval.intersects(target)) {
            result.add(node.interval.name);
        }

        if (node.left != null && Arrays.compare(node.left.max, target.low) >= 0) {
            searchOverlap(node.left, target, result);
        }

        searchOverlap(node.right, target, result);
    }

    public List<String> searchContaining(byte[] key) {
        List<String> result = new ArrayList<>();
        search(root, key, result);
        return result;
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

