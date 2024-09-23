package sstIo;

import Level.Level;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class SSTHeader implements AutoCloseable {
    private final Level level;
    private final byte checksumType;
    private final byte compressionType;
    private final byte clusterKeyCount;
    private final byte shortestCommonPrefixUsed;
    private int numberOfEntries;
    private final byte sstVersion;
    private int pointersPosition;
    private int filterPosition;
    public static final int TOTAL_HEADER_SIZE = 18;

    public SSTHeader(byte sstVersion, Level level, byte checksumType,
                     byte compressionType, byte clusterKeyCount,
                     byte shortestCommonPrefixUsed) {
        this.sstVersion = sstVersion;
        this.level = level;
        this.checksumType = checksumType;
        this.compressionType = compressionType;
        this.clusterKeyCount = clusterKeyCount;
        this.shortestCommonPrefixUsed = shortestCommonPrefixUsed;
        this.numberOfEntries = -1;
        this.pointersPosition = -1;
        this.filterPosition = -1;
    }

    public SSTHeader(byte sstVersion,
                     byte level,
                     byte checksumType,
                     byte compressionType,
                     byte clusterKeyCount,
                     byte shortestCommonPrefixUsed,
                     int numberOfEntries,
                     int filterPosition,
                     int pointersPosition) {
        this.sstVersion = sstVersion;
        this.level = Level.fromID(level);
        this.checksumType = checksumType;
        this.compressionType = compressionType;
        this.clusterKeyCount = clusterKeyCount;
        this.shortestCommonPrefixUsed = shortestCommonPrefixUsed;
        this.numberOfEntries = numberOfEntries;
        this.filterPosition = filterPosition;
        this.pointersPosition = pointersPosition;
    }

    public SSTHeader(SSTHeader header) {
        this.sstVersion = header.sstVersion;
        this.level = header.level;
        this.checksumType = header.checksumType;
        this.compressionType = header.compressionType;
        this.clusterKeyCount = header.clusterKeyCount;
        this.shortestCommonPrefixUsed = header.shortestCommonPrefixUsed;
        this.numberOfEntries = header.numberOfEntries;
        this.filterPosition = header.filterPosition;
        this.pointersPosition = header.pointersPosition;
    }

    @Override
    public void close() throws Exception {
        if (pointersPosition <= 0 || numberOfEntries <= 0) {
            throw new RuntimeException("Header without bs and number of entries found");
        }
    }

    public void storeAsBytes(ChannelBackedWriter writer) {
        validateIfAllFieldTakenIntoConsideration(9);
        writer.putByte(sstVersion)
                .putByte(level.value())
                .putByte(checksumType)
                .putByte(compressionType)
                .putByte(clusterKeyCount)
                .putByte(shortestCommonPrefixUsed)
                .putInt(numberOfEntries)
                .putInt(filterPosition)
                .putInt(pointersPosition);
    }

    public int totalHeaderSize() {
        validateIfAllFieldTakenIntoConsideration(9);
        return  Byte.BYTES + // version
                Byte.BYTES + // level
                Byte.BYTES + // checksum type
                Byte.BYTES + // compression type
                Byte.BYTES + // number of keys in chunk
                Byte.BYTES + // shortest common prefix used
                Integer.BYTES + // number of entries
                Integer.BYTES + // filter
                Integer.BYTES;
    }

    public static SSTHeader getHeader(ChannelBackedReader reader) {
        var bytes = new byte[TOTAL_HEADER_SIZE];
        reader.getBytes(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return new SSTHeader(buffer.get(), buffer.get(), buffer.get(), buffer.get(), buffer.get(), buffer.get(), buffer.getInt(), buffer.getInt(), buffer.getInt());
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

    public void setEntries(int count) {
        this.numberOfEntries = count;
    }

    public void setFilterPosition(int position) {
        this.filterPosition = position;
    }

    public void setPointersPosition(int position) {
        this.pointersPosition = position;
    }

    public void writeRemaining(ChannelBackedWriter writer) {
        writer.position(Byte.BYTES * 6);
        writer.putInt(numberOfEntries)
                .putInt(filterPosition)
                .putInt(pointersPosition);
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

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    public byte getSstVersion() {
        return sstVersion;
    }

    public int getPointersPosition() {
        return pointersPosition;
    }

    public int getFilterPosition() {
        return filterPosition;
    }

    @Override
    public String toString() {
        return "SSTHeader{" +
                "level=" + level +
                ", checksumType=" + checksumType +
                ", compressionType=" + compressionType +
                ", clusterKeyCount=" + clusterKeyCount +
                ", shortestCommonPrefixUsed=" + shortestCommonPrefixUsed +
                ", numberOfEntries=" + numberOfEntries +
                ", sstVersion=" + sstVersion +
                ", pointersPosition=" + pointersPosition +
                ", filterPosition=" + filterPosition +
                '}';
    }
}
