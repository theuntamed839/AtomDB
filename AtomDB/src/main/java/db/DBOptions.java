package db;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Comparator;

public final class DBOptions {
    private boolean verifyChecksum;
    private boolean disableCompression;

    private int pageSize;

    private int getPageSize() {
        if (pageSize == 0) {
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
        return pageSize;
    }
    private Comparator<byte[]> comparator = DBComparator.byteArrayComparator;

    public boolean isDisableCompression() {
        return disableCompression;
    }

    public void disableCompression(boolean disableCompression) {
        this.disableCompression = disableCompression;
    }

    public void setComparator(Comparator<byte[]> comparator) {
        this.comparator = comparator;
    }

}
