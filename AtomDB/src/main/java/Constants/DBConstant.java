package Constants;

import util.SizeOf;

public class DBConstant {
    public static final int INITIAL_BUFFER_SIZE = 4 * SizeOf.MB;
    public static final byte SST_VERSION = 11;
    public static final double SPARSE_BINARY_KEY_PERCENTAGE =  0.8;
}
