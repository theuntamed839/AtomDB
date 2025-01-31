package org.g2n.atomdb.db;

import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class KVUnit implements Comparable<KVUnit> {
    private final byte[] key;
    private final byte[] value;
    private final DeletionStatus isDeleted;
    private final int unitSize;


    public enum DeletionStatus {
        DELETED, NOT_DELETED;

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

    public void uploadKV(ExpandingByteBuffer buffer) {
        buffer.putInt(Integer.BYTES + key.length + Byte.BYTES + (value != null ? Integer.BYTES + value.length : 0))
                .putInt(key.length)
                .put(key)
                .put(isDeleted.value());
        if (DeletionStatus.NOT_DELETED == isDeleted) {
            buffer.putInt(value.length).put(value);
        }
    }

    public static KVUnit read(MappedByteBuffer reader) {
        int totalKvLength = reader.getInt();
        int kLenght = reader.getInt();
        var k = new byte[kLenght];
        reader.get(k);
        var isDeleted = DeletionStatus.of(reader.get());
        if (DeletionStatus.NOT_DELETED == isDeleted) {
            int vLenght = reader.getInt();
            var v = new byte[vLenght];
            reader.get(v);
            return new KVUnit(k, v);
        }
        return new KVUnit(k);
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