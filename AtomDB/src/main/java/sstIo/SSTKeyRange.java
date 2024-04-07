package sstIo;

import Checksum.Checksum;
import Checksum.Crc32cChecksum;
import db.DBComparator;
import util.Util;

public class SSTKeyRange {
    private final byte[] first;
    private final byte[] last;
    private final long checksum;
    private final int size;

    public SSTKeyRange(byte[] first, byte[] last) {
        Validate(first, last);
        this.first = first;
        this.last = last;
        Checksum checksumProvide = new Crc32cChecksum();
        this.checksum = checksumProvide.compute(first, last);
        this.size = first.length + last.length + Integer.BYTES * 2 + Long.BYTES;
    }

    public SSTKeyRange(byte[] first, byte[] last, long checksumProvided) {
        Validate(first, last);
        long computedChecksum = computeChecksum(first, last);
        Util.requireTrue(checksumProvided == computedChecksum, "Checksum mismatch");
        this.first = first;
        this.last = last;
        this.checksum = computedChecksum;
        this.size = first.length + last.length + Integer.BYTES * 2 + Long.BYTES;
    }

    private static long computeChecksum(byte[] first, byte[] last) {
        Checksum checksumProvide = new Crc32cChecksum();
        return checksumProvide.compute(first, last);
    }


    private void Validate(byte[] first, byte[] last) {
        Util.requireTrue(DBComparator.byteArrayComparator.compare(first, last) < 0, "First key should be smaller than last key");
    }

    public int getRequiredSizeToStoreKeyRange() {
        return size;
    }

    public void storeAsBytes(ChannelBackedWriter writer) {
        writer.putInt(first.length)
                .putBytes(first)
                .putInt(last.length)
                .putBytes(last)
                .putLong(checksum);
    }

    public byte[] getFirst() {
        return first;
    }

    public byte[] getLast() {
        return last;
    }

    public long getChecksum() {
        return checksum;
    }

    public boolean inRange(byte[] key) {
        return DBComparator.byteArrayComparator.compare(first, key) <= 0 &&
                DBComparator.byteArrayComparator.compare(last, key) >= 0;
    }

    public boolean overLapping(SSTKeyRange sstKeyRange) {
        return inRange(sstKeyRange.getFirst()) || inRange(sstKeyRange.getLast()) ||
                sstKeyRange.inRange(first) || sstKeyRange.inRange(last);
    }
}
