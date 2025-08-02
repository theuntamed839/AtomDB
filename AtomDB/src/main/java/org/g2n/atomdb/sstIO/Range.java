package org.g2n.atomdb.sstIO;

import com.google.common.base.Preconditions;
import org.g2n.atomdb.checksum.AtomChecksum;
import org.g2n.atomdb.checksum.Crc32cChecksum;
import org.g2n.atomdb.db.DBComparator;

import java.util.Arrays;

/**
 *
 * we can make a generic keyRange class, and sstKeyRange as the derived class.
 */

public class Range {
    private final byte[] smallest;
    private final byte[] greatest;
    private final long checksum;
    private final int size;

    public Range(byte[] smallest, byte[] greatest) {
        Preconditions.checkArgument(smallest != null && greatest != null, "Smallest and greatest keys cannot be null");
        Preconditions.checkArgument(DBComparator.byteArrayComparator.compare(smallest, greatest) < 0, "Smallest and greatest keys cannot be empty");
        this.smallest = smallest;
        this.greatest = greatest;
        AtomChecksum checksumProvide = new Crc32cChecksum();
        this.checksum = checksumProvide.compute(smallest, greatest);
        this.size = smallest.length + greatest.length + Integer.BYTES * 2 + Long.BYTES;
    }

    public byte[] getSmallest() {
        return smallest;
    }

    public byte[] getGreatest() {
        return greatest;
    }

    // TODO unit test these methods
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
