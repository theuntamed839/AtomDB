package io.github.theuntamed839.datastore4j.constants;

public class DBConstant {
    public static final int MB = 1024 * 1024;
    public static final long MEMTABLE_SIZE = 8 * MB;
    public static final long COMPACTED_SST_FILE_SIZE = 3 * MB;
    public static final long MIN_SST_FILE_SIZE = MB;
    public static final byte SST_VERSION = 11;
    public static final String OBSOLETE = "OBSOLETE";
    public static final long MARK_FILE_END = 1234567890L;
    public enum CHECKSUM_TYPE {
//        NONE((byte) 0),
        CRC32C((byte) 32);

        private final byte value;

        CHECKSUM_TYPE(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static CHECKSUM_TYPE fromValue(byte value) {
            return switch (value) {
//                case 0 -> NONE;
                case 32 -> CRC32C;
                default -> throw new IllegalArgumentException("Unknown checksum type value: " + value);
            };
        }
    }

    public enum COMPRESSION_TYPE {
        NONE((byte) 0),
        SNAPPY((byte) 1),
        LZ4((byte) 4);

        private final byte value;

        COMPRESSION_TYPE(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        public static COMPRESSION_TYPE fromValue(byte value) {
            return switch (value) {
                case 0 -> NONE;
                case 1 -> SNAPPY;
                case 4 -> LZ4;
                default -> throw new IllegalArgumentException("Unknown checksum type value: " + value);
            };
        }
    }

    public static final byte CLUSTER_SIZE = 10; // 10% of keys in memory.
    public static final boolean SHORTEST_COMMON_PREFIX_USED = true;
    public static final long KEY_VALUE_CACHE_SIZE = 100 * MB;
    public static final String DB_LOCK_FILE = "db.lock";
    public static final int IS_USED = 1;
    public static final int IS_NOT_USED = 0;
}
