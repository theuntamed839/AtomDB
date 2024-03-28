package Constants;

import util.BytesConverter;
import util.SizeOf;

public class DBConstant {
    public static final int INITIAL_BUFFER_SIZE = 4 * SizeOf.MB;
    public static final byte SST_VERSION = 11;
    public static final double SPARSE_BINARY_KEY_PERCENTAGE =  0.8;
    public static final String OBSOLETE = "OBSOLETE";
    public static final String LOG = "LOG";
    public static final int PAGE_SIZE = 4096;
    public static final byte[] MARK_FILE_END = BytesConverter.bytes(1234567890L);
}
