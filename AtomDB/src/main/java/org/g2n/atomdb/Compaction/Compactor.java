package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Mem.ImmutableMem;
import org.g2n.atomdb.Table.Table;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DbOptions;
import org.g2n.atomdb.db.KVUnit;
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

    private Collection<SSTInfo> getOverlappingSSTs(Level level) {
        SortedSet<SSTInfo> currentLevelSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSet = table.getSSTInfoSet(level.nextLevel());

        byte[] lastCompactedKey = table.getLastCompactedKey(level);

        Collection<SSTInfo> overlapping = (lastCompactedKey != null)
                ? findOverlapping(lastCompactedKey, currentLevelSet, nextLevelSet)
                : Collections.emptyList();

        if (overlapping.size() > 1) {
            return overlapping;
        }

        return findOverlapsByBoundaryKeys(currentLevelSet, nextLevelSet);
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

        try (var iterator = new CollectiveIndexedClusterIterator(Collections.unmodifiableCollection(overlappingFiles))) {
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

//    private Level performCompaction(Level level, Collection<SSTInfo> overlappingFiles) {
//        ongoingCompactions.add(level);
//        long start = System.nanoTime();
//        System.out.println(level + " org.g2n.atomdb.Compaction Started " + Thread.currentThread().getName());
//
//        try (var iterator = new CollectiveSStIterator(Collections.unmodifiableCollection(overlappingFiles), dbOptions)) {
//            sstPersist.writeManyFiles(level.nextLevel(), iterator, getMaxNumOfEntriesInSingleSST(overlappingFiles));
//            overlappingFiles.forEach(table::removeSST);
//        } catch (Exception e) {
//            logger.error("Error during compaction for level {}: {}", level, e.getMessage());
//            return level;
//        } finally {
//            ongoingCompactions.remove(level);
//        }
//        System.out.println(level + " org.g2n.atomdb.Compaction Ended   " + Thread.currentThread().getName() + " took=" + (System.nanoTime() - start) / 1_000_000_000.0 + " Seconds");
//        return level;
//    }

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
