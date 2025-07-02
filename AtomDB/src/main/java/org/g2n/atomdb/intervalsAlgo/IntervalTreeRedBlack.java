package org.g2n.atomdb.intervalsAlgo;

import org.g2n.atomdb.Table.SSTInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        if (h.left != null && Arrays.compare(h.left.max, h.max) > 0) {
            h.max = h.left.max;
        }
        if (h.right != null && Arrays.compare(h.right.max, h.max) > 0) {
            h.max = h.right.max;
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

    public List<SSTInfo> searchOverlappingReturnString(byte[] queryPoint) {
        List<SSTInfo> result = new ArrayList<>();
        searchReturnStrings(root, queryPoint, result);
        return result;
    }

    private void searchReturnStrings(Node node, byte[] point, List<SSTInfo> result) {
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

    @Override
    public void remove(Interval interval) {
        if (!contains(root, interval)) return;
        if (!isRed(root.left) && !isRed(root.right)) root.red = true;
        root = delete(root, interval);
        if (root != null) root.red = false;
    }


    private Node delete(Node h, Interval interval) {
        if (Arrays.compare(interval.low, h.interval.low) < 0) {
            if (h.left != null) {
                if (!isRed(h.left) && !isRed(h.left.left)) {
                    h = moveRedLeft(h);
                }
                h.left = delete(h.left, interval);
            }
        } else {
            if (isRed(h.left)) {
                h = rotateRight(h);
            }

            if (Arrays.compare(interval.low, h.interval.low) == 0 &&
                    h.interval.equals(interval) &&
                    h.right == null) {
                return h.left;  // Return left child if exists
            }

            if (h.right != null) {
                if (!isRed(h.right) && !isRed(h.right.left)) {
                    h = moveRedRight(h);
                }

                if (Arrays.compare(interval.low, h.interval.low) == 0 &&
                        h.interval.equals(interval)) {
                    Node min = getMin(h.right);
                    h.interval = min.interval;
                    h.right = deleteMin(h.right);
                } else {
                    h.right = delete(h.right, interval);
                }
            }
        }

        return balance(h);
    }
    private Node moveRedLeft(Node h) {
        flipColors(h);
        if (h.right !=null && isRed(h.right.left)) {
            h.right = rotateRight(h.right);
            h = rotateLeft(h);
            flipColors(h);
        }
        return h;
    }

    private Node moveRedRight(Node h) {
        flipColors(h);
        if (h.left != null && isRed(h.left.left)) {
            h = rotateRight(h);
            flipColors(h);
        }
        return h;
    }

    private Node balance(Node h) {
        if (isRed(h.right)) h = rotateLeft(h);
        if (isRed(h.left) && isRed(h.left.left)) h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right)) flipColors(h);

        updateMax(h);
        return h;
    }

    private Node getMin(Node h) {
        while (h.left != null) h = h.left;
        return h;
    }

    private Node deleteMin(Node h) {
        if (h.left == null) return null;
        if (!isRed(h.left) && !isRed(h.left.left)) h = moveRedLeft(h);
        h.left = deleteMin(h.left);
        return balance(h);
    }

    private boolean contains(Node h, Interval interval) {
        while (h != null) {
            int cmp = Arrays.compare(interval.low, h.interval.low);
            if (cmp < 0) h = h.left;
            else if (cmp > 0) h = h.right;
            else {
                if (h.interval.equals(interval)) return true;
                h = h.right; // if lows are equal, compare further (needed if duplicates)
            }
        }
        return false;
    }
}
