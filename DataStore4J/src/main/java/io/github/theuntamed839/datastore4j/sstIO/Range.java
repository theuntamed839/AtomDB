package io.github.theuntamed839.datastore4j.sstIO;

import com.google.common.base.Preconditions;
import java.util.zip.Checksum;
import java.util.zip.CRC32C;
import io.github.theuntamed839.datastore4j.db.DBComparator;

import java.util.Arrays;

public class Range {
    private static final ThreadLocal<Checksum> crc32cThreadLocal = ThreadLocal.withInitial(CRC32C::new);
    private final byte[] smallest;
    private final byte[] greatest;
    private final long checksum;
    private final int size;

    public Range(byte[] smallest, byte[] greatest) {
        Preconditions.checkArgument(smallest != null && greatest != null, "Smallest and greatest keys cannot be null");
        Preconditions.checkArgument(DBComparator.byteArrayComparator.compare(smallest, greatest) <= 0, "Smallest and greatest keys cannot be empty");
        this.smallest = smallest;
        this.greatest = greatest;
        this.checksum = getChecksum(smallest, greatest);
        this.size = smallest.length + greatest.length + Integer.BYTES * 2 + Long.BYTES;
    }

    public byte[] getSmallest() {
        return smallest;
    }

    public byte[] getGreatest() {
        return greatest;
    }

    public boolean inRange(byte[] key) {
        return DBComparator.byteArrayComparator.compare(smallest, key) <= 0 &&
                DBComparator.byteArrayComparator.compare(greatest, key) >= 0;
    }

    public boolean overlapsWith(Range other) {
        return DBComparator.byteArrayComparator.compare(this.smallest, other.getGreatest()) <= 0 &&
                DBComparator.byteArrayComparator.compare(this.greatest, other.getSmallest()) >= 0;
    }

    public boolean contains(Range range) {
        return DBComparator.byteArrayComparator.compare(this.smallest, range.smallest) <= 0 &&
                DBComparator.byteArrayComparator.compare(this.greatest, range.greatest) >= 0;
    }

    public long getChecksum(byte[] key, byte[] value) {
        var crc32c = crc32cThreadLocal.get();
        crc32c.reset();
        crc32c.update(key);
        crc32c.update(value);
        return crc32c.getValue();
    }

    @Override
    public String toString() {
        return "Range{" +
                "smallest=" + Arrays.toString(smallest) +
                ", greatest=" + Arrays.toString(greatest) +
                ", checksum=" + checksum +
                ", size=" + size +
                '}';
    }
}
