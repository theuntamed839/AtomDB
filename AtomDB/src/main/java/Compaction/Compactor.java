package Compaction;

import Level.Level;
import Mem.ImmutableMem;
import Table.Table;
import Table.SSTInfo;
import db.DbOptions;
import db.KVUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * 3. choose the one which has many deleted entries. ( we have count the number of deleted entries in sst and store in the header.)
 * 4. we can have a hit count for sst, which can tell us how optimized the sst is. if more success hits then we might not consider
 * the file for compaction and choose the one with less hit success. (hit success is finding and getting data)
 * 5. always take from oldest to newest. ( basically find sst based on above and use the oldest to newest sort)
 * 6. add the algorithm which tells how many elements can be present inside 2 key ranges.
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

        SortedSet<SSTInfo> currentLevelSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSet = table.getSSTInfoSet(level.nextLevel());

        byte[] lastCompactedKey = table.getLastCompactedKey(level);
        Iterator<SSTInfo> descendingIterator = ((TreeSet<SSTInfo>) currentLevelSet).descendingIterator();

        Collection<SSTInfo> overlapping = (lastCompactedKey != null)
                ? getOverlapping(level, lastCompactedKey, currentLevelSet, nextLevelSet)
                : Collections.emptyList();

        while (overlapping.size() <= 1 && descendingIterator.hasNext()) {
            SSTInfo current = descendingIterator.next();
            overlapping = getOverlapping(level, current.getSstKeyRange().getGreatest(), currentLevelSet, nextLevelSet);
            if (overlapping.size() <= 1) {
                overlapping = getOverlapping(level, current.getSstKeyRange().getSmallest(), currentLevelSet, nextLevelSet);
            }
            if (overlapping.size() > 1) {
                break;
            }
        }

        if (overlapping.size() <= 1) {
            System.out.println("WOW".repeat(100));
            return;
        }

        Collection<SSTInfo> tempOverlappingFiles = overlapping;
        CompletableFuture.supplyAsync(() -> performCompaction(level, tempOverlappingFiles), executors).thenApply(returnedLevel -> {
            if (Level.LEVEL_SEVEN != returnedLevel) {
                tryCompaction(returnedLevel.nextLevel());
            }
            return CompletableFuture.completedFuture(null);
        });
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
        System.out.println(level + " Compaction Started " + Thread.currentThread().getName());

        try (var iterator = new CollectiveSStIterator(Collections.unmodifiableCollection(overlappingFiles), dbOptions)) {
            sstPersist.writeManyFiles(level.nextLevel(), iterator);
            overlappingFiles.forEach(table::removeSST);
        } catch (Exception e) {
            logger.error("Error during compaction for level {}: {}", level, e.getMessage());
            return level;
        } finally {
            ongoingCompactions.remove(level);
        }
        System.out.println(level + " Compaction Ended   " + Thread.currentThread().getName() +
                " took=" + (System.nanoTime() - start) / 1_000_000_000.0 + " Seconds");
        return level;
    }

    private Collection<SSTInfo> getOverlapping(Level level, byte[] common, SortedSet<SSTInfo> currentLevelSet, SortedSet<SSTInfo> nextLevelSet) {
        Collection<SSTInfo> overlappingFiles = new ArrayList<>();
        overlappingFiles.addAll(getFilesContainingKey(common, nextLevelSet));
        overlappingFiles.addAll(getFilesContainingKey(common, currentLevelSet));
        return overlappingFiles;
    }

    private byte[] getLastCompactedKey(Level level, SortedSet<SSTInfo> levelFileSet) {
        byte[] lastCompactedKey = table.getLastCompactedKey(level);
        return lastCompactedKey != null ? lastCompactedKey : levelFileSet.getLast().getSstKeyRange().getGreatest();
    }

    private Collection<SSTInfo> getFilesContainingKey(byte[] key, SortedSet<SSTInfo> levelFileSet) {
        if (key == null) {
            return Collections.emptyList();
        }
        return levelFileSet.stream()
                .filter(sstInfo -> sstInfo.getSstKeyRange().inRange(key))
                .toList();
    }

    @Override
    public void close() throws Exception {
        executors.close();
        executors.shutdown();
    }
}
