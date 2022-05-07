package Compaction;

import java.util.LinkedList;

public class Container {
    private final LinkedList<byte[][]> list;
    private int count;
    public Container() {
        this.list = new LinkedList<>();
        count = 0;
    }

    public void add(byte[][] keyValue) throws Exception {
        if (count >= 2) {
            throw new Exception("do entry done");
        }
        list.add(keyValue);
        count++;
    }

    public byte[][] getFirst() {
        return list.getFirst();
    }

    public byte[][] getSecond() {
        return list.get(1);
    }
}
