package Level;

import util.SizeOf;

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

    public Level next() {
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
            case LEVEL_ZERO -> 10L * SizeOf.MB;
            case LEVEL_ONE -> 100L * SizeOf.MB;
            case LEVEL_TWO -> 1000L * SizeOf.MB;
            case LEVEL_THREE -> 10000L * SizeOf.MB;
            case LEVEL_FOUR -> 100000L * SizeOf.MB;
            case LEVEL_FIVE -> 1000000L * SizeOf.MB;
            case LEVEL_SIX -> 10000000L * SizeOf.MB;
            case LEVEL_SEVEN -> 100000000L * SizeOf.MB;
        };
    }
}