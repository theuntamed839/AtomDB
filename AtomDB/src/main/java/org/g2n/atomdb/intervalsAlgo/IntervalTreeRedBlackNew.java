package org.g2n.atomdb.intervalsAlgo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IntervalTreeRedBlackNew implements IntervalAlgo {

    static class Node {
        // FIX: Each node stores intervals with the same 'low' key in a list.
        List<Interval> intervals;
        byte[] key; // The 'low' value, used for tree ordering.
        byte[] max; // The max 'high' value in this node's subtree.
        Node left, right;
        boolean red;

        Node(Interval interval) {
            this.intervals = new ArrayList<>();
            this.intervals.add(interval);
            this.key = interval.low;
            this.max = interval.high;
            this.red = true; // New nodes are red by default.
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
        h.red = !h.red;
        if (h.left != null) h.left.red = !h.left.red;
        if (h.right != null) h.right.red = !h.right.red;
    }

    // FIX: The updateMax function must now consider all intervals in the current node's list.
    private void updateMax(Node h) {
        if (h == null) return;

        // Start with the max from the current node's intervals.
        if(h.intervals.isEmpty()) return;

        h.max = h.intervals.get(0).high;
        for(int i = 1; i < h.intervals.size(); i++) {
            if (Arrays.compare(h.intervals.get(i).high, h.max) > 0) {
                h.max = h.intervals.get(i).high;
            }
        }

        // Check against children's max values.
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

    @Override
    public void insert(Interval interval) {
        root = insert(root, interval);
        if (root != null) root.red = false;
    }

    private Node insert(Node h, Interval interval) {
        if (h == null) return new Node(interval);

        int cmp = Arrays.compare(interval.low, h.key);

        if (cmp < 0) {
            h.left = insert(h.left, interval);
        } else if (cmp > 0) {
            h.right = insert(h.right, interval);
        } else {
            // FIX: Key matches. Add interval to this node's list instead of creating a new node.
            h.intervals.add(interval);
        }

        // Standard LLRB balancing logic
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

//    @Override
//    public List<SSTInfo> searchOverlappingReturnString(byte[] queryPoint) {
//        List<SSTInfo> result = new ArrayList<>();
//        List<Interval> intervalResult = getIntervals(queryPoint);
//        for(Interval i : intervalResult) {
//            result.add(i.name);
//        }
//        return result;
//    }

    // FIX: The search logic is corrected for both performance and multiple intervals per node.
    private void search(Node node, byte[] point, List<Interval> result) {
        if (node == null) {
            return;
        }

        // 1. Check all intervals in the current node for an overlap.
        for (Interval interval : node.intervals) {
            if (interval.contains(point)) {
                result.add(interval);
            }
        }

        // 2. Search left subtree only if it might contain an overlapping interval.
        if (node.left != null && Arrays.compare(node.left.max, point) >= 0) {
            search(node.left, point, result);
        }

        // 3. Search right subtree only if it might contain an overlapping interval.
        // This was the performance bug. We only go right if the point is >= the node's key.
        if (Arrays.compare(point, node.key) >= 0) {
            search(node.right, point, result);
        }
    }

    @Override
    public void remove(Interval interval) {
        if (root == null) return;
        // Pre-check for contains is not strictly necessary but can be a small optimization.

        // Ensure root is not a 2-node before starting deletion.
        if (!isRed(root.left) && !isRed(root.right)) {
            root.red = true;
        }
        root = delete(root, interval);
        if (root != null) root.red = false;
    }

    private Node delete(Node h, Interval interval) {
        if (h == null) return null;

        int cmp = Arrays.compare(interval.low, h.key);

        if (cmp < 0) {
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

            // FIX: Handle deletion from node's list OR full node deletion.
            if (Arrays.compare(interval.low, h.key) == 0) {
                // Attempt to remove the specific interval from the list.
                boolean removed = h.intervals.remove(interval);

                // If the list is now empty AND we actually removed something, then we must delete the entire node.
                if (removed && h.intervals.isEmpty()) {
                    if (h.right == null) {
                        return null; // This node is gone.
                    }
                    if (!isRed(h.right) && !isRed(h.right.left)) {
                        h = moveRedRight(h);
                    }
                    // Find successor
                    Node min = getMin(h.right);
                    // Replace current node's data with successor's data
                    h.key = min.key;
                    h.intervals = min.intervals;
                    // Delete the successor
                    h.right = deleteMin(h.right);
                }
                // If the list is NOT empty after removal, we're done with this node.
                // We just need to fall through to the balancing step.
            } else { // cmp > 0
                if (h.right != null) {
                    if (!isRed(h.right) && !isRed(h.right.left)) {
                        h = moveRedRight(h);
                    }
                    h.right = delete(h.right, interval);
                }
            }
        }

        return balance(h);
    }

    private Node getMin(Node h) {
        if (h == null) return null;
        while (h.left != null) h = h.left;
        return h;
    }

    private Node deleteMin(Node h) {
        if (h.left == null) return null;
        if (!isRed(h.left) && !isRed(h.left.left)) {
            h = moveRedLeft(h);
        }
        h.left = deleteMin(h.left);
        return balance(h);
    }

    private Node moveRedLeft(Node h) {
        flipColors(h);
        if (h.right != null && isRed(h.right.left)) {
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
        if (h == null) return null;
        if (isRed(h.right)) h = rotateLeft(h);
        if (isRed(h.left) && isRed(h.left.left)) h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right)) flipColors(h);

        updateMax(h);
        return h;
    }
}
