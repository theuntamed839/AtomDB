package Constants;

import com.google.common.primitives.Longs;

public class DBConstants {
    public static final int MB = 1_000_000;
    public static final byte[] EOF = new byte[] {-1, -1, -1, -1, -1, -1, -1, -1};
    public final static int LongLength = Longs.toByteArray(1L).length;
}
