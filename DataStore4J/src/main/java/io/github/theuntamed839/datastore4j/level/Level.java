package io.github.theuntamed839.datastore4j.level;

import io.github.theuntamed839.datastore4j.constants.DBConstant;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Level implements Comparable<Level> {
    private static final Map<Integer, Level> map = new ConcurrentHashMap<>();
    public static final Level LEVEL_ZERO = of(0);
    public static final Level LEVEL_ONE = of(1);

    public static Level of(int id) {
        if (id < 0) {
            throw new IllegalArgumentException("Level ID must be >= 0");
        }
        return map.computeIfAbsent(id, Level::new);
    }

    private final long maxNumberOfFilesSupported;
    private final long levelSSTSize;
    private final AtomicBoolean toggle = new AtomicBoolean(true);
    private final int id;

    private Level(int id) {
        this.id = id;
        this.levelSSTSize = DBConstant.COMPACTED_SST_FILE_SIZE * (id + 1); // level zero doesn't support this, the size is depended upon the memtable size
        this.maxNumberOfFilesSupported = (long) ((Math.pow(10, id + 1) * DBConstant.MB) / levelSSTSize);
    }

    public static Collection<Level> values() {
        return Collections.unmodifiableCollection(map.values());
    }

    public Level nextLevel() {
        return of(this.id + 1);
    }

    public Byte value() {
        return (byte) this.id;
    }

    public long getMaxNumberOfFilesSupported() {
        return maxNumberOfFilesSupported;
    }

    public long getLevelSSTFileSize() {
        return levelSSTSize;
    }

    public boolean shouldPerformMajorCompaction() {
        boolean current = toggle.get();
        toggle.set(!current);
        return current;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Level level)) return false;

        return id == level.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public boolean isLowerLevelComparedTo(Level fromLevel) {
        return this.id < fromLevel.id;
    }

    @Override
    public int compareTo(Level level) {
        if (level == null) {
            return 1; // should never occur this state
        }
        return Integer.compare(this.id, level.id);
    }

    @Override
    public String toString() {
        return "Level_" + id;
    }
}
