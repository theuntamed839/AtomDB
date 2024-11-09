package db;

import Compression.DataCompressionStrategy;
import Compression.Lz4Compression;
import Constants.DBConstant;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Comparator;

public final class DbOptions {

    public int pageSize;
    public int memtableSize = DBConstant.MEMTABLE_SIZE;
    public Comparator<byte[]> comparator = DBComparator.byteArrayComparator;

    public DataCompressionStrategy dataCompressionStrategy = Lz4Compression.getInstance();

    public DbOptions() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe;
            unsafe = (Unsafe)f.get(null);
            pageSize = unsafe.pageSize();
        } catch (IllegalAccessException | NoSuchFieldException ignored) {
            pageSize = 4096;
        }
    }
}
