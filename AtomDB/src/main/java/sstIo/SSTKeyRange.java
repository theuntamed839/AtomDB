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
        this.size = first.length + last.length + Integer.BYTES * 2 + Long.BYTES;
    }

    public SSTKeyRange(byte[] first, byte[] last, long checksumProvided) {
        Checksum checksumProvide = new Crc32cChecksum();
        var computedChecksum = checksumProvide.compute(first, last);
        if (computedChecksum != checksumProvided) {
            throw new RuntimeException("Checksum mismatch");
        }
        this.first = first;
        this.last = last;
        this.checksum = computedChecksum;
        this.size = first.length + last.length + Integer.BYTES * 2 + Long.BYTES;
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
}
