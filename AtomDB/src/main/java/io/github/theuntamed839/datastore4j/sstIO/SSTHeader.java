package io.github.theuntamed839.datastore4j.sstIO;

import com.google.common.base.Preconditions;
import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.constants.DBConstant.CHECKSUM_TYPE;
import io.github.theuntamed839.datastore4j.constants.DBConstant.COMPRESSION_TYPE;
import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.ExpandingByteBuffer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static io.github.theuntamed839.datastore4j.constants.DBConstant.IS_NOT_USED;
import static io.github.theuntamed839.datastore4j.constants.DBConstant.IS_USED;

public class SSTHeader{
    private final Level level;
    private final CHECKSUM_TYPE checksumType;
    private final COMPRESSION_TYPE compressionType;
    private final byte singleClusterSize;
    private final boolean shortestCommonPrefixUsed;
    private int numberOfEntries;
    private final byte sstVersion;
    private int pointersPosition;
    private int filterPosition;
    public static final int TOTAL_HEADER_SIZE = 18;
    

    public static SSTHeader getDefault(Level level, DbComponentProvider dbComponentProvider) {
        return new SSTHeader(
                DBConstant.SST_VERSION,
                level.value(),
                dbComponentProvider.getChecksumType(),
                dbComponentProvider.getCompressionType(),
                dbComponentProvider.getClusterSize(),
                DBConstant.SHORTEST_COMMON_PREFIX_USED,
                -1, -1, -1);
    }

    public SSTHeader(byte sstVersion,
                     byte level,
                     CHECKSUM_TYPE checksumType,
                     COMPRESSION_TYPE compressionType,
                     byte singleClusterSize,
                     boolean shortestCommonPrefixUsed,
                     int numberOfEntries,
                     int filterPosition,
                     int pointersPosition) {
        this.sstVersion = sstVersion;
        this.level = Level.of(level);
        this.checksumType = checksumType;
        this.compressionType = compressionType;
        this.singleClusterSize = singleClusterSize;
        this.shortestCommonPrefixUsed = shortestCommonPrefixUsed;
        this.numberOfEntries = numberOfEntries;
        this.filterPosition = filterPosition;
        this.pointersPosition = pointersPosition;
    }

    protected SSTHeader(SSTHeader header) {
        validate(header);
        this.sstVersion = header.sstVersion;
        this.level = header.level;
        this.checksumType = header.checksumType;
        this.compressionType = header.compressionType;
        this.singleClusterSize = header.singleClusterSize;
        this.shortestCommonPrefixUsed = header.shortestCommonPrefixUsed;
        this.numberOfEntries = header.numberOfEntries;
        this.filterPosition = header.filterPosition;
        this.pointersPosition = header.pointersPosition;
    }

    public static SSTHeader getHeader(IOReader reader) throws IOException {
        var bytes = new byte[TOTAL_HEADER_SIZE];
        reader.read(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return new SSTHeader(buffer.get(),
                buffer.get(),
                CHECKSUM_TYPE.fromValue(buffer.get()),
                COMPRESSION_TYPE.fromValue(buffer.get()),
                buffer.get(),
                isShortestCommonPrefixUsed(buffer),
                buffer.getInt(),
                buffer.getInt(),
                buffer.getInt());
    }

    private static boolean isShortestCommonPrefixUsed(ByteBuffer buffer) {
        return buffer.get() == IS_USED;
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
    
    public void writeSSTHeaderData(ExpandingByteBuffer writer) {
        validateIfAllFieldTakenIntoConsideration(9); // todo should we remove this safety stuff ?
        validate(this);
        writer.put(sstVersion)
                .put(level.value())
                .put(checksumType.getValue())
                .put(compressionType.getValue())
                .put(singleClusterSize)
                .put((byte) (shortestCommonPrefixUsed ? IS_USED : IS_NOT_USED)) //todo
                .putInt(numberOfEntries)
                .putInt(filterPosition)
                .putInt(pointersPosition);
    }

    public Level getLevel() {
        return level;
    }

    public byte getSingleClusterSize() {
        return singleClusterSize;
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    public int getPointersPosition() {
        return pointersPosition;
    }

    public int getFilterPosition() {
        return filterPosition;
    }

    public boolean isShortestCommonPrefixUsed() {
        return shortestCommonPrefixUsed == DBConstant.SHORTEST_COMMON_PREFIX_USED;
    }

    public COMPRESSION_TYPE getCompressionStrategy() {
        return compressionType;
    }

    public CHECKSUM_TYPE getChecksumType() {
        return checksumType;
    }

    private static void validate(SSTHeader header) {
        Preconditions.checkArgument(header.singleClusterSize > 0, "Number of keys in single cluster must be greater than 0, found " + header.singleClusterSize);
        Preconditions.checkArgument(header.filterPosition <= header.pointersPosition, "Filter position must be greater than or equal to pointers position, filterPosition: " + header.filterPosition + ", pointersPosition: " + header.pointersPosition);
        Preconditions.checkArgument(header.numberOfEntries > 0, "Number of entries must be greater than 0");
        Preconditions.checkArgument(header.pointersPosition > 0, "Pointers position must be greater than 0");
    }

    private void validateIfAllFieldTakenIntoConsideration(int fields) { // todo remove
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

    @Override
    public String toString() {
        return "SSTHeader{" +
                "level=" + level.toString() +
                ", checksumType=" + checksumType +
                ", compressionType=" + compressionType +
                ", singleClusterSize=" + singleClusterSize +
                ", shortestCommonPrefixUsed=" + shortestCommonPrefixUsed +
                ", numberOfEntries=" + numberOfEntries +
                ", sstVersion=" + sstVersion +
                ", pointersPosition=" + pointersPosition +
                ", filterPosition=" + filterPosition +
                '}';
    }

    public byte sstVersion() {
        return sstVersion;
    }
}
