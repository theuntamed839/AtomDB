package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Mem.ImmutableMem;
import org.g2n.atomdb.Table.Table;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DbOptions;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.SSTKeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * 3. choose the one which has many deleted entries. ( we have count the number of deleted entries in org.g2n.atomdb.sst and store in the header.)
 * 4. we can have a hit count for org.g2n.atomdb.sst, which can tell us how optimized the org.g2n.atomdb.sst is. if more success hits then we might not consider
 * the fileToWrite for compaction and choose the one with less hit success. (hit success is finding and getting data)
 */
public class Compactor implements AutoCloseable {
    private final Table table;
    private final DbOptions dbOptions;
    private final SSTPersist sstPersist;
    private final Set<Level> ongoingCompactions = ConcurrentHashMap.newKeySet();
    private final ExecutorService executors = Executors.newCachedThreadPool();
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class.getName());

    public Compactor(Table table, DbOptions dbOptions) {
        this.table = table;
        this.dbOptions = dbOptions;
        this.sstPersist = new SSTPersist(table);
    }

    public void persistLevel0(ImmutableMem<byte[], KVUnit> memtable) throws IOException {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, memtable.getNumberOfEntries(), memtable.getKeySetIterator());
    }

    /**
     *
     * TODO:
     * Compactions needs more thought
     * we have to make sure that we always compact the old files and those files which doesn't overlap with the same level file.
     *
     * consider that level 1 has 2 files named A and B. where B is the new file and A is the old file.
     * B has 23 and 29 number in it.
     * A has 23 number in it.
     * consider that we choose the files having 29 to compact, and we search for files having 29 and compact to Level 2.
     * Now the issue starts, when we try to find number 23 we first look into the Level 1 and since we have file named A containing 23 in it we return that.
     * But turns out the newest value for 23 was in B which is now compacted to Level 2
     *
     * So we have to make sure that all the olds files are compacted first and if we can't compact the old files, then we should choose the files which are not overlapping
     * with the old files.
     */

    public synchronized void tryCompaction(Level level) {
        if (shouldSkipCompaction(level)) {
            return;
        }
        Collection<SSTInfo> overlapping = getOverlappingSSTs(level);

        if (overlapping.size() <= 1) {
            System.out.println("WOW".repeat(100));
            return;
        }
        CompletableFuture.supplyAsync(() -> performCompaction(level, overlapping), executors).thenApply(returnedLevel -> {
            if (Level.LEVEL_SEVEN != returnedLevel) {
                tryCompaction(returnedLevel.nextLevel());
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    private Collection<SSTInfo> alternative3(Level level) {
        System.out.println("Alternative 2");
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());

        var set = new TreeSet<SSTInfo>();
        for (SSTInfo sst1: currentLevelSSTSet) {
            for (SSTInfo sst2: currentLevelSSTSet) {
                if (sst1.equals(sst2)) continue;
                int overlap = calculateOverlap(sst1.getSstKeyRange(), sst2.getSstKeyRange());
                if (overlap > 0) {
                    set.add(sst1);
                    set.add(sst2);
                }
            }
        }
        var finalSet = new TreeSet<SSTInfo>();
        for (SSTInfo currLevelSST : set) {
            for (SSTInfo nextLevelSST : nextLevelSSTSet) {
                int overlap = calculateMaxNumOfKeysOverlap(currLevelSST.getSstKeyRange(), nextLevelSST.getSstKeyRange());
                if (overlap > 0) {
                    finalSet.add(nextLevelSST);
                }
            }
        }
        set.addAll(finalSet);
        System.out.println("number of selected files=" + set.size());
        return set;
    }

    private Collection<SSTInfo> alternative2(Level level) {
        System.out.println("Alternative 2");
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());


        for (SSTInfo currLevelSST : currentLevelSSTSet.reversed()) {
            var map = new HashMap<SSTInfo, Integer>();
            for (SSTInfo nextLevelSST : nextLevelSSTSet) {
                int overlap = calculateMaxNumOfKeysOverlap(currLevelSST.getSstKeyRange(), nextLevelSST.getSstKeyRange());
                if (overlap > 0) {
//                    finalSet.add(nextLevelSST);
                    map.put(nextLevelSST, overlap);
                }
            }
            if (!map.isEmpty()) {
                int avg = map.values().stream().mapToInt(each -> each).sum() / map.size();
                var myset = new TreeSet<SSTInfo>();
                myset.add(currLevelSST);
                map.entrySet().stream().filter(each -> each.getValue() >= avg).forEach(each -> myset.add(each.getKey()));
                return myset;
            }
        }

        var finalSet = new TreeSet<SSTInfo>();
        for (SSTInfo currLevelSST : currentLevelSSTSet) {
            for (SSTInfo nextLevelSST : currentLevelSSTSet) {
                if (currLevelSST.equals(nextLevelSST)) continue;
                int overlap = calculateMaxNumOfKeysOverlap(currLevelSST.getSstKeyRange(), nextLevelSST.getSstKeyRange());
                if (overlap > 0) {
                    finalSet.add(nextLevelSST);
                }
            }
        }

        return finalSet;
    }

    private int calculateMaxNumOfKeysOverlap(SSTKeyRange r1, SSTKeyRange r2) {
        Comparator<byte[]> comparator = DBComparator.byteArrayComparator;
        if (comparator.compare(r1.end(), r2.start()) <= 0 || comparator.compare(r2.end(), r1.start()) <= 0) {
            return 0; // No overlap
        }

        byte[] left = (comparator.compare(r1.start(), r2.start()) > 0) ? r1.start() : r2.start();
        byte[] right = (comparator.compare(r1.end(), r2.end()) < 0) ? r1.end() : r2.end();

        int keysOverlap = 0;
        int minLength = Math.min(left.length, right.length);
        for (int i = 0; i < minLength; i++) {
            int result = left[i] - right[i];
            if (result != 0) {
                keysOverlap += 128 + ( -1 * Math.min(left[i], right[i]));
                break;
            }
        }
        if (minLength < left.length) {
            keysOverlap += (minLength - left.length) * 256;
        }
        if (minLength < right.length) {
            keysOverlap += (minLength - right.length) * 256;
        }
        return keysOverlap;
    }


    private Collection<SSTInfo> getOverlappingSSTsAlternative(Level level) {
        System.out.println("Finding overlapping files for level" + level);
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());

        var map = new HashMap<Map.Entry<SSTInfo, SSTInfo>, Integer>();
        var sum = 0;
        var count = 0;
        for (SSTInfo currLevelSST : currentLevelSSTSet) {
            for (SSTInfo nextLevelSST : nextLevelSSTSet) {
                int overlap = calculateOverlap(currLevelSST.getSstKeyRange(), nextLevelSST.getSstKeyRange());
                if (overlap > 0) {
                    map.put(Map.entry(currLevelSST, nextLevelSST), overlap);
                    sum = sum + overlap;
                    count++;
                }
            }
        }

        for (SSTInfo sst1: currentLevelSSTSet) {
            for (SSTInfo sst2: currentLevelSSTSet) {
                if (sst1.equals(sst2)) continue;
                int overlap = calculateOverlap(sst1.getSstKeyRange(), sst2.getSstKeyRange());
                if (overlap < 0) System.exit(123123);
                if (overlap > 0) {
                    map.put(Map.entry(sst1, sst2), overlap);
                    sum = sum + overlap;
                    count++;
                }
            }
        }


        var avg = sum / count;
        var set = new TreeSet<SSTInfo>();
        for (Map.Entry<Map.Entry<SSTInfo, SSTInfo>, Integer> entryIntegerEntry : map.entrySet()) {
            if (entryIntegerEntry.getValue() >= avg) {
                set.add(entryIntegerEntry.getKey().getKey());
                set.add(entryIntegerEntry.getKey().getValue());
            }
        }

        return set;
    }

    private static int calculateOverlap(SSTKeyRange r1, SSTKeyRange r2) {
        // Check if ranges overlap
        Comparator<byte[]> comparator = DBComparator.byteArrayComparator;
        if (comparator.compare(r1.end(), r2.start()) <= 0 || comparator.compare(r2.end(), r1.start()) <= 0) {
            return 0; // No overlap
        }

        // Calculate overlap
        byte[] overlapStart = (comparator.compare(r1.start(), r2.start()) > 0) ? r1.start() : r2.start();
        byte[] overlapEnd = (comparator.compare(r1.end(), r2.end()) < 0) ? r1.end() : r2.end();

        return Math.abs(comparator.compare(overlapEnd, overlapStart));
    }

    private Collection<SSTInfo> getOverlappingSSTs(Level level) {
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());

        byte[] lastCompactedKey = table.getLastCompactedKey(level);

        Collection<SSTInfo> overlapping = (lastCompactedKey != null)
                ? findOverlapping(lastCompactedKey, currentLevelSSTSet, nextLevelSSTSet)
                : Collections.emptyList();

        if (overlapping.size() > 1) {
            System.out.println("we took A, size="+overlapping.size());
            return overlapping;
        }
        Collection<SSTInfo> overlapsByBoundaryKeys = findOverlapsByBoundaryKeys(currentLevelSSTSet, nextLevelSSTSet);
        System.out.println("we took B, size="+overlapsByBoundaryKeys.size());
        return overlapsByBoundaryKeys;
    }

    private Collection<SSTInfo> findOverlapsByBoundaryKeys(SortedSet<SSTInfo> currentLevelSet, SortedSet<SSTInfo> nextLevelSet) {
        for (Iterator<SSTInfo> it = ((TreeSet<SSTInfo>) currentLevelSet).descendingIterator(); it.hasNext(); ) {
            SSTInfo sstInfo = it.next();
            byte[] greatestKey = sstInfo.getSstKeyRange().getGreatest();
            byte[] smallestKey = sstInfo.getSstKeyRange().getSmallest();

            var overlapping = findOverlapping(greatestKey, currentLevelSet, nextLevelSet);
            if (overlapping.size() > 1) {
                return overlapping;
            }

            overlapping = findOverlapping(smallestKey, currentLevelSet, nextLevelSet);
            if (overlapping.size() > 1) {
                return overlapping;
            }
        }
        return Collections.emptyList();
    }

    private Collection<SSTInfo> findOverlapping(byte[] common, SortedSet<SSTInfo> currentLevelSet, SortedSet<SSTInfo> nextLevelSet) {
        if (common == null) {
            return Collections.emptyList();
        }
        Collection<SSTInfo> overlappingFiles = new ArrayList<>();
        addFilesContainingKey(overlappingFiles, common, nextLevelSet);
        addFilesContainingKey(overlappingFiles, common, currentLevelSet);
        return overlappingFiles;
    }

    private void addFilesContainingKey(Collection<SSTInfo> collection, byte[] key, SortedSet<SSTInfo> levelFileSet) {
        levelFileSet.stream()
                .filter(sstInfo -> sstInfo.getSstKeyRange().inRange(key))
                .forEach(collection::add);
    }

    private boolean shouldSkipCompaction(Level level) {
        int size = table.getCurrentLevelSize(level);
        return size <= level.limitingSize() ||
                ongoingCompactions.contains(level) ||
                ongoingCompactions.contains(level.nextLevel());
    }

    private Level performCompaction(Level level, Collection<SSTInfo> overlappingFiles) {
        ongoingCompactions.add(level);
        long start = System.nanoTime();
        System.out.println(level + " org.g2n.atomdb.Compaction Started " + Thread.currentThread().getName());
        try {
            var iterator = new MergedClusterIterator(Collections.unmodifiableCollection(overlappingFiles));
            sstPersist.writeManyFiles(level.nextLevel(), iterator, getAverageNumOfEntriesInSST(overlappingFiles));
            overlappingFiles.forEach(table::removeSST);
        } catch (Exception e) {
            logger.error("Error during compaction for level {}: {}", level, e.getMessage());
            e.printStackTrace();
            System.exit(123);
//            throw new RuntimeException(e);
//            return level;
        } finally {
            ongoingCompactions.remove(level);
        }
        System.out.println(level + " org.g2n.atomdb.Compaction Ended   " + Thread.currentThread().getName() + " took=" + (System.nanoTime() - start) / 1_000_000_000.0 + " Seconds");
        return level;
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
        executors.close();
        executors.shutdown();
    }
}
