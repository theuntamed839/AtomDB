package org.example.intervalsAlgo;

import java.util.*;

public class IntervalTreeRedBlack implements IntervalAlgo {

    static class Node {
        Interval interval;
        byte[] max;
        Node left, right;
        boolean red;

        Node(Interval interval) {
            this.interval = interval;
            this.max = interval.high;
            this.red = true; // default new node is red
        }
    }

    private Node root;

    private boolean isRed(Node x) {
        return x != null && x.red;
    }

    private Node rotateLeft(Node h) {
        Node x = h.right;
        h.right = x.left;
        x.left = h;
        x.red = h.red;
        h.red = true;
        updateMax(h);
        updateMax(x);
        return x;
    }

    private Node rotateRight(Node h) {
        Node x = h.left;
        h.left = x.right;
        x.right = h;
        x.red = h.red;
        h.red = true;
        updateMax(h);
        updateMax(x);
        return x;
    }

    private void flipColors(Node h) {
        h.red = true;
        if (h.left != null) h.left.red = false;
        if (h.right != null) h.right.red = false;
    }

    private void updateMax(Node h) {
        if (h == null) return;
        h.max = h.interval.high;
        if (h.left != null) {
            if (Arrays.compare(h.left.max, h.max) > 0) {
                h.max = h.left.max;
            }
        }
        if (h.right != null) {
            if (Arrays.compare(h.right.max, h.max) > 0) {
                h.max = h.right.max;
            }
        }
    }

    public void insert(Interval interval) {
        root = insert(root, interval);
        root.red = false;
    }

    private Node insert(Node h, Interval interval) {
        if (h == null) return new Node(interval);
        if (Arrays.compare(interval.low, h.interval.low) < 0) {
            h.left = insert(h.left, interval);
        } else {
            h.right = insert(h.right, interval);
        }
        if (isRed(h.right) && !isRed(h.left)) h = rotateLeft(h);
        if (isRed(h.left) && isRed(h.left.left)) h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right)) flipColors(h);

        updateMax(h);
        return h;
    }

    @Override
    public List<Interval> getIntervals(byte[] key) {
        List<Interval> result = new ArrayList<>();
        search(root, key, result);
        return result;
    }

    public List<String> searchOverlappingReturnString(byte[] queryPoint) {
        List<String> result = new ArrayList<>();
        searchReturnStrings(root, queryPoint, result);
        return result;
    }

    private void searchReturnStrings(Node node, byte[] point, List<String> result) {
        if (node == null) return;

        if (node.interval.contains(point)) {
            result.add(node.interval.name);
        }

        if (node.left != null && Arrays.compare(node.left.max, point) >= 0) {
            searchReturnStrings(node.left, point, result);
        }

        searchReturnStrings(node.right, point, result);
    }

    private void search(Node node, byte[] point, List<Interval> result) {
        if (node == null) return;

        if (node.interval.contains(point)) {
            result.add(node.interval);
        }

        if (node.left != null && Arrays.compare(node.left.max, point) >= 0) {
            search(node.left, point, result);
        }

        search(node.right, point, result);
    }

    public void delete(Interval interval) {
        root = delete(root, interval);
    }

    private Node delete(Node node, Interval interval) {
        if (node == null) return null;

        if (Arrays.compare(interval.low, node.interval.low) < 0){
            node.left = delete(node.left, interval);
        } else if ((Arrays.compare(interval.low, node.interval.low) > 0) || interval.high != node.interval.high) {
            node.right = delete(node.right, interval);
        } else {
            // match
            if (node.right == null) return node.left;
            if (node.left == null) return node.right;

            Node min = getMin(node.right);
            node.interval = min.interval;
            node.right = delete(node.right, min.interval);
        }

        updateMax(node);
        return node;
    }

    private Node getMin(Node node) {
        while (node.left != null) node = node.left;
        return node;
    }
}
