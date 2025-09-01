package io.github.theuntamed839.datastore4j.compaction;

import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.mem.ImmutableMem;
import io.github.theuntamed839.datastore4j.sstIO.Range;
import io.github.theuntamed839.datastore4j.table.Table;
import io.github.theuntamed839.datastore4j.table.SSTInfo;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import io.github.theuntamed839.datastore4j.sstIO.SSTPersist;
import io.github.theuntamed839.datastore4j.search.Search;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.System.Logger;

/**
 * TODO:
 *  1. we can choose the one which has many deleted entries.
 *     For that we can have the number of deleted entries in sst in the header
 *  2. We should maintain a hit count for each sst file to evaluate how optimized it is.
 *     Files with a higher hit success rate (i.e., more successful lookups) are likely well-optimized and can be skipped during compaction.
 */


public class Compactor implements AutoCloseable {
    private final Table table;
    private final Search search;
    private final DbComponentProvider dbComponentProvider;
    private final SSTPersist sstPersist;
    private final ExecutorService executors = Executors.newCachedThreadPool();
    private static final Logger logger = System.getLogger(Compactor.class.getName());
    private final Map<Level, ReentrantLock> locks = new ConcurrentHashMap<>();

    public Compactor(Table table, Search search, Path dbPath, DbComponentProvider dbComponentProvider) {
        this.table = table;
        this.search = search;
        this.dbComponentProvider = dbComponentProvider;
        this.sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
    }

    public void persistLevel0(ImmutableMem<byte[], KVUnit> memtable) throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, memtable.getNumberOfEntries(), memtable.getValuesIterator());
    }

    public void tryCompaction(Level level) {
        waitUntilLevelZeroCompactionFree(level); // otherwise main thread is just going to submit to executors, and let the files increase in level 0
        if (table.getNumberOfFilesInLevel(level) <= level.getMaxNumberOfFilesSupported()) {
            return;
        }

        if (executors.isShutdown()) {
            return;
        }
        executors.execute(() -> {
            ReentrantLock lock1 = locks.computeIfAbsent(level, k -> new ReentrantLock());
            ReentrantLock lock2 = locks.computeIfAbsent(level.nextLevel(), k -> new ReentrantLock());
            try {
                lock1.lock();
                lock2.lock();

                Collection<SSTInfo> overlapping = getCurrentAndNextLevelOverlappingFiles(level, level.shouldPerformMajorCompaction());
                if (overlapping.size() <= 1) {
                    overlapping = getCurrentAndNextLevelOverlappingFiles(level, false);
                }
                if (overlapping.size() <= 1) {
                    logger.log(Logger.Level.INFO, String.format("No overlapping files found for compaction at level %s. Skipping compaction.", level));
                    return;
                }
                performCompaction(level, overlapping);
            }
            finally {
                if (lock1.isHeldByCurrentThread()) {
                    lock1.unlock();
                }
                if (lock2.isHeldByCurrentThread()) {
                    lock2.unlock();
                }
            }
            tryCompaction(level.nextLevel());
        });
    }

    private void waitUntilLevelZeroCompactionFree(Level level) {
        if (!level.equals(Level.LEVEL_ZERO)) {
            return;
        }
        ReentrantLock lock1 = locks.computeIfAbsent(level, k -> new ReentrantLock());
        ReentrantLock lock2 = locks.computeIfAbsent(level.nextLevel(), k -> new ReentrantLock());
        lock1.lock();
        try {
            lock2.lock();
            try {
            } finally {
                lock2.unlock();
            }
        } finally {
            lock1.unlock();
        }
    }

    private Range computeCombinedRange(SortedSet<SSTInfo> sortedSet) {
        byte[] greatestKey = sortedSet.getLast().getSstKeyRange().getGreatest();
        byte[] smallestKey = sortedSet.getLast().getSstKeyRange().getSmallest();
        for(SSTInfo sstInfo : sortedSet) {
            if (dbComponentProvider.getComparator().compare(greatestKey, sstInfo.getSstKeyRange().getGreatest()) < 0) {
                greatestKey = sstInfo.getSstKeyRange().getGreatest();
            }
            if (dbComponentProvider.getComparator().compare(smallestKey, sstInfo.getSstKeyRange().getSmallest()) > 0) {
                smallestKey = sstInfo.getSstKeyRange().getSmallest();
            }
        }
        return new Range(smallestKey, greatestKey);
    }

    private Collection<SSTInfo> getCurrentAndNextLevelOverlappingFiles(Level level, boolean performMajorCompaction) {
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        if (currentLevelSSTSet.isEmpty()) {
            return Collections.emptyList();
        }

        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());
        for (SSTInfo sstInfo : currentLevelSSTSet.reversed()) {
            var set = getAllOverlappingFilesToCompactWithGivenRange(sstInfo.getSstKeyRange(), currentLevelSSTSet, nextLevelSSTSet, performMajorCompaction);
            if (set.size() > 1) {
                return set;
            }
        }
        return List.of();
    }

    private SortedSet<SSTInfo> getAllOverlappingFilesToCompactWithGivenRange(Range range, SortedSet<SSTInfo> currentLevelSSTSet,
                                                                             SortedSet<SSTInfo> nextLevelSSTSet, boolean performMajorCompaction) {
        /*
         IMPORTANT:
            Ideally, we want to select:
            All overlapping files from the current level, along with any files they further overlap with.
            All overlapping files from the next level that overlap with the current level files, including any files they further overlap with.

            This approach would lead to highly optimized SSTs, but it could also be a very expensive process,
            as it might require compacting almost all the SSTs from both levels.

            To mitigate this:
                1. Avoid newer overlapping files from the current level.
            (Since reading is done with the newest files first, we won’t disrupt the structure. If we include newer files while leaving older ones,
             then compaction will push these newer files to the next level, causing reads to prioritize older files first—breaking the intended order.)

                2. Avoid older overlapping files from the next level.
            (Why avoid older files? Consider if we take older files and skip newer ones,
             the compaction from the previous level will introduce newly compacted files as newer files in this level.
              This would disrupt the read order, since reads would then encounter older values first.)
         */

        SortedSet<SSTInfo> initiallyOverlapping = new TreeSet<>();
        for (SSTInfo sst : currentLevelSSTSet) {
            if (range.overlapsWith(sst.getSstKeyRange())) {
                initiallyOverlapping.add(sst);
            }
        }

        SortedSet<SSTInfo> expandedOverlappingSet = expandOverlappingFiles(currentLevelSSTSet, initiallyOverlapping);
//        var wideRange = computeCombinedRange(expandedOverlappingSet);
        expandedOverlappingSet.addAll(findNextLevelNonOverlappingSSTs(nextLevelSSTSet, range, performMajorCompaction));
        return expandedOverlappingSet;
    }


    private static SortedSet<SSTInfo> expandOverlappingFiles(SortedSet<SSTInfo> allSSTs, SortedSet<SSTInfo> initialOverlappingSet) {
        Deque<SSTInfo> toProcess = new ArrayDeque<>(initialOverlappingSet);
        SortedSet<SSTInfo> result = new TreeSet<>();
        while (!toProcess.isEmpty()) {
            var current = toProcess.poll();
            if (result.contains(current)) {
                continue;
            }
            for (SSTInfo candidate : allSSTs) {
                if (candidate.equals(current) || candidate.isNewerThan(current)) { //avoid newer ssts.
                    continue;
                }
                if (current.getSstKeyRange().overlapsWith(candidate.getSstKeyRange())) {
                    toProcess.add(candidate);
                }
            }
            result.add(current);
        }
        return result;
    }

    private Collection<? extends SSTInfo> findNextLevelNonOverlappingSSTs(SortedSet<SSTInfo> ssts, Range range, boolean performMajorCompaction) {
        /*
        range.overlapsWith(candidate.getSstKeyRange());
        It generates highly optimized ssts, but is very expensive, as it gets a lot of ssts to compact

        candidate.getSstKeyRange().contains(range)
        Further level ssts are highly compact, and can decide whether to get compacted based on the range

        candidate.getSstKeyRange().inRange(range.getSmallest()) || candidate.getSstKeyRange().inRange(range.getGreatest());
         */
        SortedSet<SSTInfo> result = new TreeSet<>();
        for (SSTInfo candidate : ssts) {
            boolean condition = performMajorCompaction ? range.overlapsWith(candidate.getSstKeyRange()) : candidate.getSstKeyRange().contains(range);
            if (condition) {
                boolean toInclude = true;
                for (SSTInfo other : ssts) {
                    if (other.equals(candidate)) {
                        continue;
                    }
//                  if sst is older than don't accept
                    if (candidate.getSstKeyRange().overlapsWith(other.getSstKeyRange()) && candidate.isOlderThan(other)) {
                        toInclude = false;
                        break;
                    }
                }
                if(toInclude) {
                    result.add(candidate);
                }
            }
        }
        return result;
    }

    private void performCompaction(Level level, Collection<SSTInfo> overlappingFiles) {
        try (var iterator = new MergedClusterIterator(Collections.unmodifiableCollection(overlappingFiles), search, dbComponentProvider)){
            sstPersist.writeManyFiles(level.nextLevel(), iterator, getAverageNumOfEntriesInSST(overlappingFiles), overlappingFiles);
        } catch (Exception e) {
            e.printStackTrace();
            logger.log(Logger.Level.ERROR,String.format("Error during compaction for level %s: %s", level, e.getMessage()));
        }
    }

    private int getAverageNumOfEntriesInSST(Collection<SSTInfo> overlappingFiles) {
        return (int) overlappingFiles.stream()
                .map(SSTInfo::getNumberOfEntries)
                .mapToInt(Integer::intValue)
                .average()
                .orElseThrow(() -> new IllegalStateException("Unexpected error during average calculation."));
    }


    @Override
    public void close() throws Exception {
        executors.shutdown();
        executors.awaitTermination(1, TimeUnit.MINUTES); // todo this return value can be used to identify if compaction was unsuccessful, which can help in deleting intermediate files.
        executors.close();
    }
}
