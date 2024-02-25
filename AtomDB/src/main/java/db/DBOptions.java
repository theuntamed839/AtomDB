package db;

import java.io.File;
import java.util.Comparator;

public final class DBOptions {
    private boolean verifyChecksum;
    private boolean disableCompression;
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
