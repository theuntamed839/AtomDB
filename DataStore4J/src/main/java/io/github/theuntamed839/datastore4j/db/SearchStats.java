package io.github.theuntamed839.datastore4j.db;

import java.util.Map;

public record SearchStats(Map<Integer, Long> fileSeekHistogram,
                          long successfulSearchCount,
                          long unsuccessfulSearchCount) {
    public long totalSearches() {
        return successfulSearchCount + unsuccessfulSearchCount;
    }

    public long totalValuesFromCache() {
        return successfulSearchCount - fileSeekHistogram.values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }
}
