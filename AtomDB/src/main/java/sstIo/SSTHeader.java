package sstIo;

import Level.Level;

import java.lang.reflect.Field;

public class SSTHeader implements AutoCloseable {
    private final Level level;
    private final byte checksumType;
    private final byte compressionType;
    private final byte clusterKeyCount;
    private final byte shortestCommonPrefixUsed;
    private long numberOfEntries;
    private final byte sstVersion;
    private final SSTKeyRange sstKeyRange;
    private long pointersPosition;
    private long filterPosition;
    public static final long SST_KEY_RANGE_POSITION_IN_HEADER = 30;

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

    public SSTHeader(byte sstVersion,
                     Level level,
                     byte checksumType,
                     byte compressionType,
                     byte clusterKeyCount,
                     byte shortestCommonPrefixUsed,
                     long numberOfEntries,
                     long filterPosition,
                     long pointersPosition,
                     SSTKeyRange sstKeyRange) {
        this.sstVersion = sstVersion;
        this.level = level;
        this.checksumType = checksumType;
        this.compressionType = compressionType;
        this.clusterKeyCount = clusterKeyCount;
        this.shortestCommonPrefixUsed = shortestCommonPrefixUsed;
        this.numberOfEntries = numberOfEntries;
        this.filterPosition = filterPosition;
        this.pointersPosition = pointersPosition;
        this.sstKeyRange = sstKeyRange;
    }

    public int totalHeaderSize() {
        validateIfAllFieldTakenIntoConsideration(10);
        return  Byte.BYTES + // version
                Byte.BYTES + // level
                Byte.BYTES + // checksum type
                Byte.BYTES + // compression type
                Byte.BYTES + // number of keys in chunk
                Byte.BYTES + // shortest common prefix used
                Long.BYTES + // number of entries
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
                .putLong(numberOfEntries)
                .putLong(filterPosition)
                .putLong(pointersPosition);
        sstKeyRange.storeAsBytes(writer);
    }

    private void validateIfAllFieldTakenIntoConsideration(int fields) {
        Field[] declaredFields = getClass().getDeclaredFields();
        int staticCount = 0;
        for (Field declaredField : declaredFields) {
            if (java.lang.reflect.Modifier.isStatic(declaredField.getModifiers())) {
                staticCount++;
            }
        }
        if (getClass().getDeclaredFields().length  - staticCount != fields) {
            throw new RuntimeException("Modified " + getClass().getName() + " but didn't modify the total count");
        }
    }

    public void setEntries(long count) {
        this.numberOfEntries = count;
    }

    public void setFilterPosition(long position) {
        this.filterPosition = position;
    }

    public void setPointersPosition(long position) {
        this.pointersPosition = position;
    }

    public void writeRemaining(ChannelBackedWriter writer) {
        writer.position(Integer.BYTES /*header size is store at start*/ + 6);
        writer.putLong(numberOfEntries)
                .putLong(filterPosition)
                .putLong(pointersPosition);
    }

    public Level getLevel() {
        return level;
    }

    public byte getChecksumType() {
        return checksumType;
    }

    public byte getCompressionType() {
        return compressionType;
    }

    public byte getClusterKeyCount() {
        return clusterKeyCount;
    }

    public byte getShortestCommonPrefixUsed() {
        return shortestCommonPrefixUsed;
    }

    public long getNumberOfEntries() {
        return numberOfEntries;
    }

    public byte getSstVersion() {
        return sstVersion;
    }

    public SSTKeyRange getSstKeyRange() {
        return sstKeyRange;
    }

    public long getPointersPosition() {
        return pointersPosition;
    }

    public long getFilterPosition() {
        return filterPosition;
    }
}
