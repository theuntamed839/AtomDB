package sstIo;

import Checksum.Checksum;
import Checksum.Crc32cChecksum;

public class SSTKeyRange {
    private final byte[] first;
    private final byte[] last;
    private final long checksum;
    private final int size;

    public SSTKeyRange(byte[] first, byte[] last) {
        this.first = first;
        this.last = last;
        Checksum checksumProvide = new Crc32cChecksum();
        this.checksum = checksumProvide.compute(first, last);
        this.size = first.length + last.length + Long.BYTES * 3;
    }

    public int getRequiredSizeToStoreKeyRange() {
        return size;
    }
}
