package org.g2n.atomdb.compaction;

import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.mem.ImmutableMem;
import org.g2n.atomdb.sstIO.Range;
import org.g2n.atomdb.table.Table;
import org.g2n.atomdb.table.SSTInfo;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIO.SSTPersist;
import org.g2n.atomdb.search.Search;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO:
 *  1. we can choose the one which has many deleted entries.
 *     For that we can have the number of deleted entries in org.g2n.atomdb.sst in the header
 *  2. We should maintain a hit count for each org.g2n.atomdb.sst file to evaluate how optimized it is.
 *     Files with a higher hit success rate (i.e., more successful lookups) are likely well-optimized and can be skipped during compaction.
 *
 *  Note:
 *   1. We must ensure that we always compact the older files first, and that we avoid compacting files that overlap with other files in the same level.
 *      consider that level 1 has 2 files named A and B. where B is the latest and A is the oldest.
 *      B has 23 and 29 number in it.
 *      A has 23 number in it.
 *      consider that we choose the files having 29 to compact, and we search for files having 29 and compact to next Level 2.
 *      Now the issue starts, when we try to find number 23 we first look into the Level 1 and since we have file A containing 23 in it we return that.
 *      But turns out the newest value for 23 was in B which is now compacted to Level 2
 *      So we have to make sure that all the olds files are compacted first and if we can't compact the old files, then we should choose the files which are not overlapping
 *      with the old files.
 */


public class Compactor implements AutoCloseable {
    private final Table table;
    private final Search search;
    private final DbComponentProvider dbComponentProvider;
    private final SSTPersist sstPersist;
    private final AtomicInteger numberOfActuallyCompactions = new AtomicInteger(0);
    private final ExecutorService executors = Executors.newCachedThreadPool();
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class.getName());
    private final Map<Level, ReentrantLock> locks = new HashMap<>();

    public Compactor(Table table, Search search, Path dbPath, DbComponentProvider dbComponentProvider) {
        this.table = table;
        this.search = search;
        this.dbComponentProvider = dbComponentProvider;
        this.sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
        for (Level level : Level.values()) {
            locks.put(level, new ReentrantLock());
        }
    }

    public void persistLevel0(ImmutableMem<byte[], KVUnit> memtable) throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, memtable.getNumberOfEntries(), memtable.getKeySetIterator());
    }

    public void tryCompaction(Level level) {
        if (table.getCurrentLevelSize(level) < level.limitingSize()) {
            return;
        }

        waitUntilLevelZeroCompactionFree(level); // otherwise main thread is just going to submit to executors, and let the files increase in level 0
        if (executors.isShutdown()) {
            return;
        }
        executors.execute(() -> {
            ReentrantLock lock1 = locks.get(level);
            ReentrantLock lock2 = locks.get(level.nextLevel());
            try {
                lock1.lock();
                lock2.lock();
                Collection<SSTInfo> overlapping = getCurrentAndNextLevelOverlappingFiles(level);
                if (overlapping.size() <= 1) {
                    return;
                }
                performCompaction(level, overlapping);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (lock1.isHeldByCurrentThread()) {
                    lock1.unlock();
                }
                if (lock2.isHeldByCurrentThread()) {
                    lock2.unlock();
                }
            }
            if (Level.LEVEL_SEVEN != level.nextLevel()) {
                tryCompaction(level.nextLevel());
            }
        });
    }

    private void waitUntilLevelZeroCompactionFree(Level level) {
        if (level != Level.LEVEL_ZERO) {
            return;
        }
        ReentrantLock lock1 = locks.get(level);
        ReentrantLock lock2 = locks.get(level.nextLevel());
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
            if (Arrays.compare(greatestKey, sstInfo.getSstKeyRange().getGreatest()) < 0) {
                greatestKey = sstInfo.getSstKeyRange().getGreatest();
            }
            if (Arrays.compare(smallestKey, sstInfo.getSstKeyRange().getSmallest()) > 0) {
                smallestKey = sstInfo.getSstKeyRange().getSmallest();
            }
        }
        return new Range(smallestKey, greatestKey);
    }

    private Collection<SSTInfo> getCurrentAndNextLevelOverlappingFiles(Level level) {
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        if (currentLevelSSTSet.isEmpty()) {
            return Collections.emptyList();
        }

        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());
        for (SSTInfo sstInfo : currentLevelSSTSet.reversed()) {
            var set = getAllOverlappingFilesToCompactWithGivenRange(sstInfo.getSstKeyRange(), currentLevelSSTSet, nextLevelSSTSet);
            if (set.size() > 1) {
                return set;
            }
        }
        return List.of();
    }

    private SortedSet<SSTInfo> getAllOverlappingFilesToCompactWithGivenRange(Range range, SortedSet<SSTInfo> currentLevelSSTSet,
                                                                             SortedSet<SSTInfo> nextLevelSSTSet) {
        /*
            IMPORTANT:
            Ideally, we want to select:
            All overlapping files from the current level, along with any files they further overlap with.
            All overlapping files from the next level with the current level files, including their further overlapping files.

            These would lead to highly optimized SSTs but could be very expensive process too as we might take all the SSTs from both the levels.

            To mitigate this,
            1.We can avoid the newer overlapping files from the current level.
             (since reading is done with the newest first, we are not messing the structure, if we take newer files and keep the older files, then compaction
              is going to push these new files to next level, thus reading is going to happen with the older files first)

            2.We can avoid taking older overlapping files from the next level.
             (Why avoid older files ? consider opposite, we take older files and avoid newer files, since it's the compaction from the previous level, newly
             compacted files will be pushed as newer files in this level, leading to messed up reading structure, as we're going to read the older values first)
         */

        SortedSet<SSTInfo> initiallyOverlapping = new TreeSet<>();
        for (SSTInfo sst : currentLevelSSTSet) {
            if (range.overlapsWith(sst.getSstKeyRange())) {
                initiallyOverlapping.add(sst);
            }
        }

        SortedSet<SSTInfo> expandedOverlappingSet = expandOverlappingFiles(currentLevelSSTSet, initiallyOverlapping);
        var wideRange = computeCombinedRange(expandedOverlappingSet);
        expandedOverlappingSet.addAll(findNextLevelNonOverlappingSSTs(nextLevelSSTSet, wideRange));
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

    private Collection<? extends SSTInfo> findNextLevelNonOverlappingSSTs(SortedSet<SSTInfo> ssts, Range range) {
        SortedSet<SSTInfo> result = new TreeSet<>();
        for (SSTInfo candidate : ssts) {
            // the older method seems to be working faster, with less efficiency of compacting, but faster compactions.
            boolean oldMethod = true && (candidate.getSstKeyRange().inRange(range.getSmallest()) || candidate.getSstKeyRange().inRange(range.getGreatest()));
            boolean newMethod = true; //&& range.overlapsWith(candidate.getSstKeyRange());
            // todo: in new method, experiment and see what happens if we directly move older files from a level which is full which doesn't have any overlapping files among them.
            if (oldMethod && newMethod) {
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
        numberOfActuallyCompactions.addAndGet(1);
        try {
            var iterator = new MergedClusterIterator(Collections.unmodifiableCollection(overlappingFiles), search, dbComponentProvider);
            sstPersist.writeManyFiles(level.nextLevel(), iterator, getAverageNumOfEntriesInSST(overlappingFiles));
            table.removeSST(overlappingFiles);
        } catch (Exception e) {
            logger.error("Error during compaction for level {}: {}", level, e.getMessage());
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
        executors.awaitTermination(1, TimeUnit.MINUTES); // todo this return value can be used to identify if compaction was unsuccessful, which can help in deleting delete files.
        executors.close();
        System.out.println("Number of actually compactions: " + numberOfActuallyCompactions.get());
        // number of files in each levels
        for (Level level : Level.values()) {
            System.out.println(level + " has " + table.getCurrentLevelSize(level) + " size in bytes");
            System.out.println(level + " has " + table.getSSTInfoSet(level).size() + " files");
        }
    }
}
