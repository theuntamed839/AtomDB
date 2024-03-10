package sst;

import sstIo.Reader;

public class KeyUnit {
    public static final byte DELETE = 1;
    public static final byte ADDED = 0;
    private byte[] key;
    private byte isDelete;

    private int valueSize;
    public KeyUnit(byte[] key, byte isDelete, int valueSize) {
        this.key = key;
        this.isDelete = isDelete;
        this.valueSize = valueSize;
    }

    public byte[] getKey() {
        return key;
    }

    public byte getIsDelete() {
        return isDelete;
    }

    public int getValueSize() {
        return valueSize;
    }
}
