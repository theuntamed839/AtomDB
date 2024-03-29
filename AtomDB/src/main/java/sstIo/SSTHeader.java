package sstIo;

import Level.Level;

public class SSTHeader {
    private final Level level;
    private final byte crc32cChecksumType;
    private final byte lz4CompressionType;
    private final byte numberOfKeysInChunk;
    private final byte shortestCommonPrefixUsed;
    private final int numberOfEntries;
    private final byte sstVersion;
    private final SSTKeyRange sstKeyRange;

    public SSTHeader(byte sstVersion, Level level, byte crc32cChecksumType,
                     byte lz4CompressionType, byte numberOfKeysInChunk,
                     byte shortestCommonPrefixUsed, int numberOfEntries, SSTKeyRange sstKeyRange) {
        this.sstVersion = sstVersion;
        this.level = level;
        this.crc32cChecksumType = crc32cChecksumType;
        this.lz4CompressionType = lz4CompressionType;
        this.numberOfKeysInChunk = numberOfKeysInChunk;
        this.shortestCommonPrefixUsed = shortestCommonPrefixUsed;
        this.numberOfEntries = numberOfEntries;
        this.sstKeyRange = sstKeyRange;
    }

    public int totalHeaderSize() {
        return  Byte.BYTES + // version
                Byte.BYTES + // level
                Byte.BYTES + // checksum type
                Byte.BYTES + // compression type
                Byte.BYTES + // number of keys in chunk
                Byte.BYTES + // shortest common prefix used
                Long.BYTES + // BS
                Integer.BYTES + // number of entries
                sstKeyRange.getTotalBoundSize();
    }

    private int getTotalBoundSize() {

    }
}
