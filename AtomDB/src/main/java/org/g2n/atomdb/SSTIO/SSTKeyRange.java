package org.g2n.atomdb.SSTIO;

import org.g2n.atomdb.Checksum.AtomChecksum;
import org.g2n.atomdb.Checksum.Crc32cChecksum;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.util.Util;

public class SSTKeyRange {
    private final byte[] smallest;
    private final byte[] greatest;
    private final long checksum;
    private final int size;

    public SSTKeyRange(byte[] smallest, byte[] greatest) {
        Validate(smallest, greatest);
        this.smallest = smallest;
        this.greatest = greatest;
        AtomChecksum checksumProvide = new Crc32cChecksum();
        this.checksum = checksumProvide.compute(smallest, greatest);
        this.size = smallest.length + greatest.length + Integer.BYTES * 2 + Long.BYTES;
    }

    public SSTKeyRange(byte[] smallest, byte[] greatest, long checksumProvided) {
        Validate(smallest, greatest);
        long computedChecksum = computeChecksum(smallest, greatest);
        Util.requireTrue(checksumProvided == computedChecksum, "AtomChecksum mismatch");
        this.smallest = smallest;
        this.greatest = greatest;
        this.checksum = computedChecksum;
        this.size = smallest.length + greatest.length + Integer.BYTES * 2 + Long.BYTES;
    }

    private static long computeChecksum(byte[] first, byte[] last) {
        AtomChecksum checksumProvide = new Crc32cChecksum();
        return checksumProvide.compute(first, last);
    }


    private void Validate(byte[] first, byte[] last) {
        Util.requireTrue(DBComparator.byteArrayComparator.compare(first, last) < 0, "First key should be smaller than greatest key");
    }

    public int getRequiredSizeToStoreKeyRange() {
        return size;
    }

    public void storeAsBytes(ChannelBackedWriter writer) {
        writer.putInt(smallest.length)
                .putBytes(smallest)
                .putInt(greatest.length)
                .putBytes(greatest)
                .putLong(checksum);
    }

    public byte[] getSmallest() {
        return smallest;
    }

    public byte[] getGreatest() {
        return greatest;
    }

    public byte[] start() {
        return smallest;
    }

    public byte[] end() {
        return greatest;
    }

    public long getChecksum() {
        return checksum;
    }

    public boolean inRange(byte[] key) {
        return DBComparator.byteArrayComparator.compare(smallest, key) <= 0 &&
                DBComparator.byteArrayComparator.compare(greatest, key) >= 0;
    }

    public boolean overLapping(SSTKeyRange givenRange) {
        return (DBComparator.byteArrayComparator.compare(smallest, givenRange.getGreatest()) <= 0 &&
                DBComparator.byteArrayComparator.compare(greatest, givenRange.getSmallest()) >= 0);
    }

    @Override
    public String toString() {
        return "SSTKeyRange{" +
                "smallest=" + new String(smallest) +
                ", greatest=" + new String(greatest) +
                ", checksum=" + checksum +
                ", size=" + size +
                '}';
    }
}
