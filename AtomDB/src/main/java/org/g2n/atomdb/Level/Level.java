package org.g2n.atomdb.Level;

import org.g2n.atomdb.Constants.DBConstant;

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
            case LEVEL_ZERO -> 10L * DBConstant.MB; // 5
            case LEVEL_ONE -> 100L * DBConstant.MB; // 50
            case LEVEL_TWO -> 1000L * DBConstant.MB; // 500
            case LEVEL_THREE -> 10000L * DBConstant.MB; // 5000
            case LEVEL_FOUR -> 100000L * DBConstant.MB;
            case LEVEL_FIVE -> 1000000L * DBConstant.MB;
            case LEVEL_SIX -> 10000000L * DBConstant.MB;
            case LEVEL_SEVEN -> 100000000L * DBConstant.MB;
        };
    }
}