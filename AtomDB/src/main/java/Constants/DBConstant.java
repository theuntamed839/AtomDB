package Constants;

import util.BytesConverter;
import util.SizeOf;

public class DBConstant {
    public static final int MEMTABLE_SIZE = 4 * SizeOf.MB;
    public static final byte SST_VERSION = 11;
    public static final double SPARSE_BINARY_KEY_PERCENTAGE =  0.8;
    public static final String OBSOLETE = "OBSOLETE";
    public static final String LOG = "LOG";
    public static final int PAGE_SIZE = 4096;
    public static final long MARK_FILE_END = 1234567890L;
    public static final byte CRC32C_CHECKSUM_TYPE = 32;
    public static final byte LZ4_COMPRESSION_TYPE = 4;
    public static final byte CLUSTER_SIZE = 10; // 10% of keys in memory.
    public static final byte SHORTEST_COMMON_PREFIX_USED = 1;


}
