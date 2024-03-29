package db;

import java.util.Arrays;
import java.util.Objects;

public class KVUnit {
    public static final byte DELETE = 1;
    public static final byte PRESENT = 0;
    private final byte[] key;
    private final byte[] value;
    private final byte isDelete;

    public KVUnit(byte[] key, byte isDelete) {
        Objects.requireNonNull(key);
        this.key = key;
        this.isDelete = isDelete;
        this.value = null;
    }

    public KVUnit(byte[] key, byte[] value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        this.key = key;
        this.value = value;
        this.isDelete = PRESENT;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public byte getIsDelete() {
        return isDelete;
    }

    public long unitSize() {
        return key.length + (value != null ? value.length : 0);
    }

    @Override
    public String toString() {
        return "KVUnit{" +
                "key=" + Arrays.toString(key) +
                ", value=" + (value != null ? Arrays.toString(value) : 0) +
                ", isDelete=" + isDelete +
                '}';
    }
}
