package org.g2n.atomdb.db;

import org.g2n.atomdb.Compression.DataCompressionStrategy;
import org.g2n.atomdb.Compression.Lz4Compression;
import org.g2n.atomdb.Constants.DBConstant;
import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.util.Comparator;

public final class DbOptions {

    public int pageSize;
    public int memtableSize = DBConstant.MEMTABLE_SIZE;
    public Comparator<byte[]> comparator = DBComparator.byteArrayComparator;

    public DataCompressionStrategy dataCompressionStrategy = Lz4Compression.getInstance();
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
        if (sstSize <= DBConstant.MIN_SST_FILE_SIZE) {
            throw new IllegalArgumentException("SST file size must be greater than " + DBConstant.MIN_SST_FILE_SIZE);
        }
        // todo
        // need to store this size and use it throughout the DB implementation
        // moreover we should have the memtableSize same as the SST file size
    }

    public void disallowUseOfMMap() {
        this.isMMapAllowed = false;
    }

    public boolean isMMapAllowed() {
        return isMMapAllowed;
    }
}
