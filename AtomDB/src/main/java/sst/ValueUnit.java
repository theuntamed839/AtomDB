package sst;

import java.util.Arrays;
import java.util.Objects;

public class ValueUnit {
    public static final short DELETE = 1;
    public static final short ADDED = 0;
    private byte[] value;
    private short isDelete;

    @Override
    public String toString() {
        return "sst.ValueUnit{" +
                "value=" + Arrays.toString(value) +
                ", isDelete=" + isDelete +
                '}';
    }

    public byte[] getValue() {
        return value;
    }

    public short getIsDelete() {
        return isDelete;
    }

    public ValueUnit(byte[] value, short isDelete) {
        Objects.requireNonNull(value);
        this.value = value;
        this.isDelete = isDelete;
    }

    public ValueUnit(short isDelete) {
        this.isDelete = isDelete;
    }
}
