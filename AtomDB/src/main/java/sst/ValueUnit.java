package sst;

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
        return "sst.ValueUnit{" +
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
        Objects.requireNonNull(value);
        this.value = value;
        this.isDelete = isDelete;
    }

    public ValueUnit(byte isDelete) {
        this.isDelete = isDelete;
    }
}
