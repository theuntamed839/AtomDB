package org.g2n.atomdb;

import org.g2n.atomdb.db.DBComparator;

import java.util.*;

public class Trial {

    public static void main(String[] args) {
        Random random = new Random();
        var intervalList = new ArrayList<Interval>();
        IntervalTree intervalTree = new IntervalTree();
        for (int i = 0; i < 100000; i++) {
            Interval val;
            byte[] l1 = new byte[random.nextInt(10, 500)];
            byte[] l2 = new byte[random.nextInt(10, 500)];
            random.nextBytes(l1); random.nextBytes(l2);
            int compare = DBComparator.byteArrayComparator.compare(l1, l2);
            if (compare < 0) {
                val = new Interval(l1, l2, getSaltString());
            }else if (compare > 0) {
                val = new Interval(l2, l1, getSaltString());
            }
            else {
                continue;
            }
            intervalTree.insert(val);
            intervalList.add(val);
        }
//        test(intervalTree, intervalList);

        var toSearchList = new ArrayList<byte[]>();
        for (int i = 0; i < 10000000; i++) {
            byte[] l1 = new byte[random.nextInt(10, 500)];
            random.nextBytes(l1);
            toSearchList.add(l1);
        }

        var start1 = System.nanoTime();
        toSearchList.forEach(each -> getAllOverlappingIntervals(intervalList, each));
        var end1 = System.nanoTime();
        System.out.println("second "+ (end1 - start1));

        long start = System.nanoTime();
        toSearchList.forEach(intervalTree::searchContaining);
        long end = System.nanoTime();
        System.out.println("first "+ (end - start));

    }

    private static void test(IntervalTree intervalTree, List<Interval> list) {
        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            boolean flag = false;
            byte[] keyToFind = new byte[random.nextInt(10, 500)];
            random.nextBytes(keyToFind);
            List<String> strings = intervalTree.searchContaining(keyToFind);
            List<String> linear = getAllOverlappingIntervals(list, keyToFind);
            System.out.println("liner " + linear.size() + " strings " + strings.size());
            if (strings.size() != linear.size()) {
                System.out.println("failed");
                System.out.println(strings);
                System.out.println(linear);
                break;
            }

            Map<String, Boolean> map = new HashMap<>();
            for (String string : strings) {
                map.put(string, true);
            }

            for (String s : linear) {
                if (!map.containsKey(s)) {
                    System.out.println("failed");
                    flag = true;
                    break;
                }
            }
            if (flag) {
                break;
            }
        }
    }

    public static List<String> getAllOverlappingIntervals(List<Interval> intervals, byte[] key) {
        List<String> list = new ArrayList<>();
        for (Interval interval : intervals) {
            if (interval.contains(key)) {
                list.add(interval.name);
            }
        }
        return list;
    }

    static String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }
}

class Interval {
    final byte[] low, high;
    final String name;

    public Interval(byte[] low, byte[] high, String name) {
        this.low = low;
        this.high = high;
        this.name = name;
    }

    public boolean contains(byte[] key) {
        return DBComparator.byteArrayComparator.compare(low, key) <= 0 &&
                DBComparator.byteArrayComparator.compare(high, key) >= 0;
    }

    public boolean intersects(Interval other) {
        return (DBComparator.byteArrayComparator.compare(low, other.high) <= 0 &&
                DBComparator.byteArrayComparator.compare(high, other.low) >= 0);
    }

    @Override
    public String toString() {
        return name;
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

class IntervalTree {
    private IntervalNode root;

    public void insert(Interval interval) {
        root = insert(root, interval);
    }

    private IntervalNode insert(IntervalNode node, Interval interval) {
        if (node == null) return new IntervalNode(interval);

        if (DBComparator.byteArrayComparator.compare(interval.low, node.interval.low) < 0 ){
            node.left = insert(node.left, interval);
        } else {
            node.right = insert(node.right, interval);
        }

        if (DBComparator.byteArrayComparator.compare(node.max, interval.high) < 0) {
            node.max = interval.high;
        }
        return node;
    }

    public List<String> searchContaining(byte[] key) {
        List<String> result = new ArrayList<>();
        search(root, key, result);
        return result;
    }

    private void search(IntervalNode node, byte[] key, List<String> result) {
        if (node == null) return;

        if (node.interval.contains(key)) {
            result.add(node.interval.name);
        }

        if (node.left != null && DBComparator.byteArrayComparator.compare(key, node.left.max) <= 0) {
            search(node.left, key, result);
        }

        search(node.right, key, result);
    }

    public List<String> searchOverlapping(byte[] low, byte[] high) {
        List<String> result = new ArrayList<>();
        searchOverlap(root, new Interval(low, high, null), result);
        return result;
    }

    private void searchOverlap(IntervalNode node, Interval target, List<String> result) {
        if (node == null) return;

        if (node.interval.intersects(target)) {
            result.add(node.interval.name);
        }

        if (node.left != null && DBComparator.byteArrayComparator.compare(node.left.max, target.low) >= 0) {
            searchOverlap(node.left, target, result);
        }

        searchOverlap(node.right, target, result);
    }
}


