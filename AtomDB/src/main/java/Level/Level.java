package Level;

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

    public Integer value() {
        return this.ordinal();
    }

    public static Level fromID(int id) {
        return levels[id];
    }

    public static long toID(Level level) {
        return level.value();
    }
}