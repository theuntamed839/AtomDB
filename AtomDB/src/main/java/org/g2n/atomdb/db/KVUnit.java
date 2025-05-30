package org.g2n.atomdb.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class KVUnit implements Comparable<KVUnit> {
    private final byte[] key;
    private final byte[] value;
    private final DeletionStatus isDeleted;
    private final int unitSize;

    public enum DeletionStatus {
        DELETED, NOT_DELETED;

        public static final int BYTES = Byte.BYTES;

        public static boolean isDeleted(byte isDeleted) {
            return switch (isDeleted) {
                case 1 -> true;
                case 0 -> false;
                default -> throw new IllegalStateException("Unexpected value: " + isDeleted);
            };
        }

        public byte value() {
            return switch (this) {
                case DELETED -> 1;
                case NOT_DELETED -> 0;
            };
        }

        public static DeletionStatus of(byte given) {
            if (given == DELETED.value()) return DELETED;
            if (given == NOT_DELETED.value()) return NOT_DELETED;
            throw new RuntimeException("not of DeletionStatus type");
        }
    }

    public KVUnit(byte[] key) {
        this(key, DeletionStatus.DELETED, null);
    }

    public KVUnit(byte[] key, byte[] value) {
        this(key, DeletionStatus.NOT_DELETED, value);
    }

    private KVUnit(byte[] key, DeletionStatus isDeleted, byte[] value) {
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        if (isDeleted == DeletionStatus.NOT_DELETED) {
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
        return key.length + (value != null ? value.length : 1 /*isDeleted marker*/);
    }

    public int getUnitSize() {
        return unitSize;
    }

    public boolean isDeleted() {
        return isDeleted == DeletionStatus.DELETED;
    }

    public DeletionStatus getDeletedStatus() {
        return this.isDeleted;
    }

    @Override
    public String toString() {
        String keyString = key.length > 10 ? Arrays.toString(Arrays.copyOf(key, 10)) + "..." : Arrays.toString(key);
        String valueString = value != null ? (value.length > 10 ? Arrays.toString(Arrays.copyOf(value, 10)) + "..." : Arrays.toString(value)) : "null";
        return String.format("KVUnit{key=%s, value=%s, isDeleted=%s}", keyString, valueString, isDeleted);
    }

    @Override
    public int compareTo(KVUnit kvUnit) {
        return DBComparator.byteArrayComparator.compare(this.key, kvUnit.key);
    }
}