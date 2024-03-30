package sstIo;

import Level.Level;

public class SSTHeader implements AutoCloseable {
    private final Level level;
    private final byte checksumType;
    private final byte compressionType;
    private final byte clusterKeyCount;
    private final byte shortestCommonPrefixUsed;
    private int numberOfEntries;
    private final byte sstVersion;
    private final SSTKeyRange sstKeyRange;
    private long pointersPosition;
    private long filterPosition;

    public SSTHeader(byte sstVersion, Level level, byte checksumType,
                     byte compressionType, byte clusterKeyCount,
                     byte shortestCommonPrefixUsed, SSTKeyRange sstKeyRange) {
        this.sstVersion = sstVersion;
        this.level = level;
        this.checksumType = checksumType;
        this.compressionType = compressionType;
        this.clusterKeyCount = clusterKeyCount;
        this.shortestCommonPrefixUsed = shortestCommonPrefixUsed;
        this.sstKeyRange = sstKeyRange;
        this.numberOfEntries = -1;
        this.pointersPosition = -1;
        this.filterPosition = -1;
    }

    public int totalHeaderSize() {
        validateIfAllFieldTakenIntoConsideration(10);
        return  Byte.BYTES + // version
                Byte.BYTES + // level
                Byte.BYTES + // checksum type
                Byte.BYTES + // compression type
                Byte.BYTES + // number of keys in chunk
                Byte.BYTES + // shortest common prefix used
                Integer.BYTES + // number of entries
                Long.BYTES + // filter
                Long.BYTES + // BS
                // partial total => 26
                sstKeyRange.getRequiredSizeToStoreKeyRange();
    }

    @Override
    public void close() throws Exception {
        if (pointersPosition <= 0 || numberOfEntries <= 0) {
            throw new RuntimeException("Header without bs and number of entries found");
        }
    }

    public void storeAsBytes(ChannelBackedWriter writer) {
        validateIfAllFieldTakenIntoConsideration(10);
        writer.putByte(sstVersion)
                .putByte(level.value())
                .putByte(checksumType)
                .putByte(compressionType)
                .putByte(clusterKeyCount)
                .putByte(shortestCommonPrefixUsed)
                .putInt(numberOfEntries)
                .putLong(filterPosition)
                .putLong(pointersPosition);
        sstKeyRange.storeAsBytes(writer);
    }

    private void validateIfAllFieldTakenIntoConsideration(int fields) {
        if (getClass().getDeclaredFields().length != fields) {
            throw new RuntimeException("Modified " + getClass().getName() + " but didn't modify the total count");
        }
    }

    public void setEntries(int count) {
        this.numberOfEntries = count;
    }

    public void setFilterPosition(long position) {
        this.filterPosition = position;
    }

    public void setPointersPosition(long position) {
        this.pointersPosition = position;
    }

    public void writeRemaining(ChannelBackedWriter writer) {
        writer.position(6);
        writer.putInt(numberOfEntries)
                .putLong(filterPosition)
                .putLong(pointersPosition);
    }
}
