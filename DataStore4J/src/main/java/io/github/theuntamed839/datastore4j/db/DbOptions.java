package io.github.theuntamed839.datastore4j.db;

import io.github.theuntamed839.datastore4j.constants.DBConstant;
import java.util.Comparator;

public final class DbOptions {
    private long memtableSize = DBConstant.MEMTABLE_SIZE;
    private Comparator<byte[]> comparator = DBComparator.byteArrayComparator;
    private byte clusterSize = DBConstant.CLUSTER_SIZE;
    private DBConstant.COMPRESSION_TYPE compressionType = DBConstant.COMPRESSION_TYPE.LZ4;
    private DBConstant.CHECKSUM_TYPE checksumType = DBConstant.CHECKSUM_TYPE.CRC32C;
    private long SSTFileSize = DBConstant.COMPACTED_SST_FILE_SIZE;
    private boolean isMMapAllowed = true;
    private long keyValueCacheSize = DBConstant.KEY_VALUE_CACHE_SIZE;

    public DbOptions() {
    }

    public void setSSTFileSizeAndMemTableSize(long sstSize, long memtableSize) {
        if (sstSize < DBConstant.MIN_SST_FILE_SIZE) {
            throw new IllegalArgumentException("SST file size must at least be" + DBConstant.MIN_SST_FILE_SIZE);
        }
        if (memtableSize < sstSize) {
            throw new IllegalArgumentException("Memtable size must be greater than or equal to SST file size");
        }
        this.memtableSize = memtableSize;
        SSTFileSize = sstSize;
    }

    public DbOptions disallowUseOfMMap() {
        this.isMMapAllowed = false;
        return this;
    }

    public DbOptions setKeyValueCacheSize(long keyValueCacheSize) {
        this.keyValueCacheSize = keyValueCacheSize;
        return this;
    }

    public DbOptions setClusterSize(byte clusterSize) {
        if (clusterSize < 2 || clusterSize > 20) {
            throw new IllegalArgumentException("Cluster size must be between 2 and 20");
        }
        // where 2 means 50% of keys in memory, and 20 means 5% of keys in memory.
        this.clusterSize = clusterSize;
        return this;
    }

//    public void setChecksumType(DBConstant.CHECKSUM_TYPE checksumType) {
//        this.checksumType = checksumType;
//    }

    public DbOptions setCompressionType(DBConstant.COMPRESSION_TYPE compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public DbOptions setComparator(Comparator<byte[]> comparator) {
        this.comparator = comparator;
        return this;
    }

    public DBConstant.CHECKSUM_TYPE getChecksumType() {
        return this.checksumType;
    }

    public DBConstant.COMPRESSION_TYPE getCompressionType() {
        return compressionType;
    }

    public long getSSTFileSize() {
        return SSTFileSize;
    }

    public Comparator<byte[]> getComparator() {
        return comparator;
    }

    public long getKeyValueCacheSize() {
        return keyValueCacheSize;
    }

    public byte getClusterSize() {
        return clusterSize;
    }

    public boolean isMMapAllowed() {
        return isMMapAllowed;
    }

    public long getMemtableSize() {
        return memtableSize;
    }

}
