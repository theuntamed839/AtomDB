package io.github.theuntamed839.datastore4j.db;

import java.util.Arrays;
import java.util.Objects;

public class KVUnit implements Comparable<KVUnit> {
    public static final int TOMBSTONE_BYTES = 1;
    private final byte[] key;
    private final byte[] value;
    private final boolean isDeleted; // isDeleted = 1, is not deleted = 0;
    private final int unitSize;

    public KVUnit(byte[] key) {
        this(key, true, null);
    }

    public KVUnit(byte[] key, byte[] value) {
        this(key, false, value);
    }

    private KVUnit(byte[] key, boolean isDeleted, byte[] value) {
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        if (!isDeleted) {
            Objects.requireNonNull(value, "Value cannot be null when not deleted");
        }
        this.isDeleted = isDeleted;
        this.value = value;
        this.unitSize = calculateUnitSize();
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    private int calculateUnitSize() {
        return key.length + (value != null ? value.length : 1 /*isTombStone marker*/);
    }

    public int getUnitSize() {
        return unitSize;
    }

    public boolean isTombStone() {
        return isDeleted;
    }

    public byte getTombStoneValue() {
        return isDeleted ? (byte) 1 : (byte) 0;
    }

    public static boolean isTombStone(byte b) {
        return b == 1;
    }

    @Override
    public String toString() {
        String keyString = key.length > 10 ? Arrays.toString(Arrays.copyOf(key, 10)) + "..." : Arrays.toString(key);
        String valueString = value != null ? (value.length > 10 ? Arrays.toString(Arrays.copyOf(value, 10)) + "..." : Arrays.toString(value)) : "null";
        return String.format("KVUnit{key=%s, value=%s, isTombStone=%s}", keyString, valueString, isDeleted);
    }

    @Override
    public int compareTo(KVUnit kvUnit) {
        return DBComparator.byteArrayComparator.compare(this.key, kvUnit.key);
    }
}