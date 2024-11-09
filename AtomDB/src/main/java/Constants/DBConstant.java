package Constants;

import util.SizeOf;

public class DBConstant {
    public static final int MEMTABLE_SIZE = 2 * SizeOf.MB;
    public static final int COMPACTED_SST_FILE_SIZE = 2 * SizeOf.MB;
    public static final int PAGE_SIZE = COMPACTED_SST_FILE_SIZE / 2; //4096;
    public static final int WRITER_BUFFER_SIZE = COMPACTED_SST_FILE_SIZE + SizeOf.MB;
    public static final byte SST_VERSION = 11;
    public static final double SPARSE_BINARY_KEY_PERCENTAGE =  0.8;
    public static final String OBSOLETE = "OBSOLETE";
    public static final long MARK_FILE_END = 1234567890L;
    public static final byte CRC32C_CHECKSUM_TYPE = 32;
    public static final byte LZ4_COMPRESSION_TYPE = 4;
    public static final byte CLUSTER_SIZE = 10; // 10% of keys in memory.
    public static final byte SHORTEST_COMMON_PREFIX_USED = 1;
    public static final long KEY_VALUE_CACHE_SIZE = 100 * 1024 * 1024;
}
