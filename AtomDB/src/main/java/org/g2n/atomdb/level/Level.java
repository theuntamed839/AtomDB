package org.g2n.atomdb.level;

import org.g2n.atomdb.constants.DBConstant;

public enum Level {
    LEVEL_ZERO,
    LEVEL_ONE,
    LEVEL_TWO,
    LEVEL_THREE,
    LEVEL_FOUR,
    LEVEL_FIVE,
    LEVEL_SIX,
    LEVEL_SEVEN;

    private static final Level[] levels = {LEVEL_ZERO,
            LEVEL_ONE,
            LEVEL_TWO,
            LEVEL_THREE,
            LEVEL_FOUR,
            LEVEL_FIVE,
            LEVEL_SIX,
            LEVEL_SEVEN};

    public Level nextLevel() {
        return switch (this) {
            case LEVEL_ZERO -> LEVEL_ONE;
            case LEVEL_ONE -> LEVEL_TWO;
            case LEVEL_TWO -> LEVEL_THREE;
            case LEVEL_THREE -> LEVEL_FOUR;
            case LEVEL_FOUR -> LEVEL_FIVE;
            case LEVEL_FIVE -> LEVEL_SIX;
            case LEVEL_SIX, LEVEL_SEVEN -> LEVEL_SEVEN;
        };
    }

    public Byte value() {
        return (byte) this.ordinal();
    }

    public static Level fromID(byte id) {
        return levels[id];
    }

    public static Level fromID(int id) {
        return levels[id];
    }

    public static byte toID(Level level) {
        return level.value();
    }

    public long limitingSize() {
        return switch (this) {
            case LEVEL_ZERO -> (10L * DBConstant.MB) / levelSSTSize();
            case LEVEL_ONE -> (100L * DBConstant.MB) / levelSSTSize();
            case LEVEL_TWO -> (1000L * DBConstant.MB) / levelSSTSize();
            case LEVEL_THREE -> (10000L * DBConstant.MB) / levelSSTSize();
            case LEVEL_FOUR -> (100000L * DBConstant.MB) / levelSSTSize();
            case LEVEL_FIVE -> (1000000L * DBConstant.MB) / levelSSTSize();
            case LEVEL_SIX -> (10000000L * DBConstant.MB) / levelSSTSize();
            case LEVEL_SEVEN -> (100000000L * DBConstant.MB) / levelSSTSize();
        };
    }

    public long levelSSTSize() {
        return switch (this) {
            case LEVEL_ZERO -> (DBConstant.COMPACTED_SST_FILE_SIZE);
            case LEVEL_ONE -> (DBConstant.COMPACTED_SST_FILE_SIZE * 2);
            case LEVEL_TWO -> (DBConstant.COMPACTED_SST_FILE_SIZE * 3);
            case LEVEL_THREE -> (DBConstant.COMPACTED_SST_FILE_SIZE * 4);
            case LEVEL_FOUR -> (DBConstant.COMPACTED_SST_FILE_SIZE * 5);
            case LEVEL_FIVE -> (DBConstant.COMPACTED_SST_FILE_SIZE * 6);
            case LEVEL_SIX -> (DBConstant.COMPACTED_SST_FILE_SIZE * 7);
            case LEVEL_SEVEN -> (DBConstant.COMPACTED_SST_FILE_SIZE * 8);
        };
    }
}