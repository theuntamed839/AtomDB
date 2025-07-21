package org.g2n.atomdb.db;

import org.g2n.atomdb.constants.DBConstant;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Comparator;

public final class DbOptions {

    public int pageSize;
    public int memtableSize = DBConstant.MEMTABLE_SIZE;
    private Comparator<byte[]> comparator = DBComparator.byteArrayComparator;
    private byte clusterSize = DBConstant.CLUSTER_SIZE;
    private DBConstant.COMPRESSION_TYPE compressionType = DBConstant.COMPRESSION_TYPE.LZ4;
    private DBConstant.CHECKSUM_TYPE checksumType = DBConstant.CHECKSUM_TYPE.CRC32C;
    private int sstFileSize = DBConstant.COMPACTED_SST_FILE_SIZE;
    private boolean isMMapAllowed = true;

    public DbOptions() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe;
            unsafe = (Unsafe)f.get(null);
            pageSize = unsafe.pageSize();
            System.out.println("From System Page size: " + pageSize);
        } catch (IllegalAccessException | NoSuchFieldException ignored) {
            pageSize = 4096;
        }
    }

    public void setSSTFileSize(int sstSize) {
        if (sstSize <= DBConstant.MIN_SST_FILE_SIZE) { // todo find the actual min size
            throw new IllegalArgumentException("SST file size must be greater than " + DBConstant.MIN_SST_FILE_SIZE);
        }
        sstFileSize = sstSize;
    }

    public void disallowUseOfMMap() {
        this.isMMapAllowed = false;
    }

    public boolean isMMapAllowed() {
        return isMMapAllowed;
    }

    public byte getClusterSize() {
        System.out.println("Cluster size: " + clusterSize);
        return clusterSize;
    }

    public void setClusterSize(byte clusterSize) {
        if (clusterSize <= 0) {
            throw new IllegalArgumentException("Cluster size must be greater than 0");
        }

        if (clusterSize >= 5 && clusterSize < 20) { // todo extremes not tested
            throw new IllegalArgumentException("Cluster size must be between 5 and 20");
        }

        this.clusterSize = clusterSize;
    }

    public DBConstant.CHECKSUM_TYPE getChecksumType() {
        return this.checksumType;
    }

    public void setChecksumType(DBConstant.CHECKSUM_TYPE checksumType) {
        this.checksumType = checksumType;
    }

    public DBConstant.COMPRESSION_TYPE getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(DBConstant.COMPRESSION_TYPE compressionType) {
        this.compressionType = compressionType;
    }

    public int getSSTSize() {
        return sstFileSize;
    }

    public Comparator<byte[]> getComparator() {
        return comparator;
    }

    public void setComparator(Comparator<byte[]> comparator) {
        this.comparator = comparator;
    }
}
