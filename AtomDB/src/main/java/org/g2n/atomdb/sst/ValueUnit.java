package org.g2n.atomdb.sst;

import java.util.Arrays;
import java.util.Objects;

/**
 *  TODO
 *  can be improved to take byte array directly and make valueUnit out of it
 */
public class ValueUnit {
    public static final byte DELETE = 1;
    public static final byte ADDED = 0;
    private byte[] value;
    private byte isDelete;

    @Override
    public String toString() {
        return "org.g2n.atomdb.sst.ValueUnit{" +
                "value=" + Arrays.toString(value) +
                ", isDelete=" + isDelete +
                '}';
    }

    public byte[] getValue() {
        return value;
    }

    public byte getIsDelete() {
        return isDelete;
    }

    public ValueUnit(byte[] value, byte isDelete) {
        // todo the isDelete from this constructor.
        Objects.requireNonNull(value);
        this.value = value;
        this.isDelete = isDelete;
    }

    public ValueUnit(byte isDelete) {
        this.isDelete = isDelete;
    }

    public int getSize() {
        return  1 + (value != null ? value.length : 0);
    }
}
