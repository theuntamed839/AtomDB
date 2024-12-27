package org.g2n.atomdb.sst;

import org.g2n.atomdb.Checksum.CheckSumStatic;
import org.g2n.atomdb.util.Util;

public class KeyUnit {
    public static final byte DELETE = 1;
    public static final byte ADDED = 0;
    private final long keyChecksum;
    private byte[] key;
    private byte isDelete;

    private int valueSize;
    public KeyUnit(byte[] key, long checkSum, byte isDelete, int valueSize) {
        Util.requireTrue(CheckSumStatic.compute(key) == checkSum, "checksum mismatch key");
        this.key = key;
        this.keyChecksum = checkSum;
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

    public long getKeyChecksum() {return keyChecksum; }
}
