package org.g2n.atomdb.SSTIO;

import com.google.common.base.Preconditions;
import org.g2n.atomdb.Checksum.AtomChecksum;
import org.g2n.atomdb.Checksum.Crc32cChecksum;
import org.g2n.atomdb.db.DBComparator;

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

    public int getRequiredSizeToStoreKeyRange() {
        return size;
    }

    public byte[] getSmallest() {
        return smallest;
    }

    public byte[] getGreatest() {
        return greatest;
    }

    public long getChecksum() {
        return checksum;
    }

    public boolean inRange(byte[] key) {
        return DBComparator.byteArrayComparator.compare(smallest, key) <= 0 &&
                DBComparator.byteArrayComparator.compare(greatest, key) >= 0;
    }

    public boolean overLapping(Range givenRange) {
        return (DBComparator.byteArrayComparator.compare(smallest, givenRange.getGreatest()) <= 0 &&
                DBComparator.byteArrayComparator.compare(greatest, givenRange.getSmallest()) >= 0);
    }

    @Override
    public String toString() {
        return "Range{" +
                "smallest=" + new String(smallest) +
                ", greatest=" + new String(greatest) +
                ", checksum=" + checksum +
                ", size=" + size +
                '}';
    }
}
