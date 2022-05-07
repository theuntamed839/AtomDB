package Compaction;
//    private static final int ZERO = 0;
//    private static final int ONE = 1;
//    private static final int TWO = 2;
//    private static final int THREE = 3;
//    private static final int FOUR = 4;
//    private static final int FIVE = 5;
//    private static final int SIX = 6;
//    private static final int SEVEN = 7;
    public enum Level {
    LEVEL_ZERO,
    LEVEL_ONE,
    LEVEL_TWO,
    LEVEL_THREE,
    LEVEL_FOUR,
    LEVEL_FIVE,
    LEVEL_SIX,
    LEVEL_SEVEN;

        public Level next(Level level) {
            return switch (level) {
                case LEVEL_ZERO -> LEVEL_ONE;
                case LEVEL_ONE -> LEVEL_TWO;
                case LEVEL_TWO -> LEVEL_THREE;
                case LEVEL_THREE -> LEVEL_FOUR;
                case LEVEL_FOUR -> LEVEL_FIVE;
                case LEVEL_SIX -> LEVEL_SEVEN;
                default -> throw new IllegalAccessError("no more levels");
            };
        }

    public Integer value() {
        return this.ordinal();
    }
}