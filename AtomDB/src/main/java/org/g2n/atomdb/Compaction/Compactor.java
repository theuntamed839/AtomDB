package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Mem.ImmutableMem;
import org.g2n.atomdb.Table.Table;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.SSTIO.SSTKeyRange;
import org.g2n.atomdb.SSTIO.SSTPersist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 3. choose the one which has many deleted entries. ( we have count the number of deleted entries in org.g2n.atomdb.sst and store in the header.)
 * 4. we can have a hit count for org.g2n.atomdb.sst, whicinsth can tell us how optimized the org.g2n.atomdb.sst is. if more success hits then we might not consider
 * the fileToWrite for compaction and choose the one with less hit success. (hit success is finding and getting data)
 */
public class Compactor implements AutoCloseable {
    private final Table table;
    private final SSTPersist sstPersist;
    private AtomicInteger numberOfActuallyCompactions = new AtomicInteger(0);
    // todo we need to shutdown this threadpool
    private final ExecutorService executors = Executors.newCachedThreadPool();
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class.getName());
    private final Map<Level, ReentrantLock> locks = new HashMap<>();

    public Compactor(Table table, Path dbPath, DbComponentProvider dbComponentProvider) {
        this.table = table;
        this.sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
        for (Level level : Level.values()) {
            locks.put(level, new ReentrantLock());
        }
    }

    public void persistLevel0(ImmutableMem<byte[], KVUnit> memtable) throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, memtable.getNumberOfEntries(), memtable.getKeySetIterator());
    }

    /**
     *
     * TODO:
     * Compactions needs more thought
     * we have to make sure that we always compact the old files and those files which doesn't overlap with the same level path.
     *
     * consider that level 1 has 2 files named A and B. where B is the new path and A is the old path.
     * B has 23 and 29 number in it.
     * A has 23 number in it.
     * consider that we choose the files having 29 to compact, and we search for files having 29 and compact to Level 2.
     * Now the issue starts, when we try to find number 23 we first look into the Level 1 and since we have path named A containing 23 in it we return that.
     * But turns out the newest value for 23 was in B which is now compacted to Level 2
     *
     * So we have to make sure that all the olds files are compacted first and if we can't compact the old files, then we should choose the files which are not overlapping
     * with the old files.
     */

    public void tryCompaction(Level level) {
        System.out.println(level + " start tryCompaction" + Thread.currentThread().getName());
        if (table.getCurrentLevelSize(level) < level.limitingSize()) {
            System.out.println(level + " not enough files to compact" + Thread.currentThread().getName());
            return;
        }

        // todo to remove
//        long sum = 0;
//        for (SSTInfo sstInfo : table.getSSTInfoSet(level)) {
//            sum += sstInfo.getFileTorsoSize();
//        }
//        if (sum < level.limitingSize()) {
//            System.out.println(level + "Why is the sum less" + Thread.currentThread().getName());
//            throw new RuntimeException(level + "Not good");
//        }
        // end

        var isLevelZero = level == Level.LEVEL_ZERO;
        while (isLevelZero && (locks.get(level).isLocked() || locks.get(level.nextLevel()).isLocked())) {
            try {
                System.out.println("level=" + level + " is locked" + Thread.currentThread().getName());
                System.out.println(locks.get(level).isLocked() + " " + locks.get(level.nextLevel()).isLocked() + Thread.currentThread().getName());
                System.out.println("waiting for zero compaction to finish" + Thread.currentThread().getName());
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(level + " providing to executors" + Thread.currentThread().getName());
        executors.execute(() -> {
            System.out.println(level + "start of the executor lambda" + Thread.currentThread().getName());
            ReentrantLock lock1 = locks.get(level);
            ReentrantLock lock2 = locks.get(level.nextLevel());
            boolean acquired = false;
            try {
                while (!acquired) {
                    boolean gotFirst = lock1.tryLock();
                    if (!gotFirst) {
                        Thread.sleep(1); // brief backoff
                        continue;
                    }

                    boolean gotSecond = lock2.tryLock();
                    if (!gotSecond) {
                        lock1.unlock(); // release and retry
                        Thread.sleep(1);
                        continue;
                    }

                    acquired = true; // both acquired
                }
                System.out.println(level + " got the locks" + Thread.currentThread().getName());
                Collection<SSTInfo> overlapping = getCurrAndNextLevelOverlappins2(level);
                if (overlapping.size() <= 1) {
                    System.out.println(level + "WOW".repeat(10) + Thread.currentThread().getName());
                    return;
                }
                System.out.println(level + "overlapping files are " + overlapping.size() + Thread.currentThread().getName());
                performCompaction(level, overlapping);
            } catch (Exception e) {
                System.out.println(level + "exception boss" + Thread.currentThread().getName());
                e.printStackTrace();
            } finally {
                System.out.println(level + " releasing the locks" + Thread.currentThread().getName());
                if (lock1.isHeldByCurrentThread()) {
                    lock1.unlock();
                    System.out.println(level + " released lock1" + Thread.currentThread().getName());
                }
                if (lock2.isHeldByCurrentThread()) {
                    lock2.unlock();
                    System.out.println(level + " released lock2" + Thread.currentThread().getName());
                }
            }
            System.out.println(level + "trying compaction for next level" + Thread.currentThread().getName());
            if (Level.LEVEL_SEVEN != level.nextLevel()) {
                tryCompaction(level.nextLevel());
            }
            System.out.println(level + "end of the executor lambda" + Thread.currentThread().getName());
        });
        System.out.println(level + " end of tryCompaction" + Thread.currentThread().getName());
    }

    private SSTKeyRange getSSTKeyRange(SortedSet<SSTInfo> sortedSet) {
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
        return new SSTKeyRange(smallestKey, greatestKey);
    }

    private SortedSet<SSTInfo> getOverlappingSSTs(SortedSet<SSTInfo> sstInfos, SSTKeyRange range) {
        var greatestKey = range.getGreatest();
        var smallestKey = range.getSmallest();

        SortedSet<SSTInfo> overlappingFiles = new TreeSet<>();
        addFilesContainingKey(overlappingFiles, greatestKey, sstInfos);
        addFilesContainingKey(overlappingFiles, smallestKey, sstInfos);
        if (overlappingFiles.isEmpty()) {
            return overlappingFiles;
        }
        var wideRange = getSSTKeyRange(overlappingFiles);

        addFilesContainingKey(overlappingFiles, wideRange.getSmallest(), sstInfos);
        addFilesContainingKey(overlappingFiles, wideRange.getGreatest(), sstInfos);
        return overlappingFiles;
    }

    private Collection<SSTInfo> getCurrAndNextLevelOverlappins(Level level) {
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());
        if (currentLevelSSTSet.isEmpty()) {
            return Collections.emptyList();
        }

        var currLevelOverlappings = getOverlappingSSTs(currentLevelSSTSet, currentLevelSSTSet.getLast().getSstKeyRange());
        if (nextLevelSSTSet.isEmpty() || currLevelOverlappings.isEmpty()) {
            return currLevelOverlappings;
        }

        var wideRange = getSSTKeyRange(currLevelOverlappings);
        var nextLevelOverlappings = getOverlappingSSTs(nextLevelSSTSet, wideRange);
        currLevelOverlappings.addAll(nextLevelOverlappings);
        return currLevelOverlappings;
    }

    private Collection<SSTInfo> getCurrAndNextLevelOverlappins1(Level level) {
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        if (currentLevelSSTSet.isEmpty()) {
            return Collections.emptyList();
        }
        var currLevelOverlappings = getTransitivelyOverlapping(currentLevelSSTSet, currentLevelSSTSet.getLast().getSstKeyRange());
        System.out.println("Level " + level + " has " + currentLevelSSTSet.size() + " files and overlapping files are " + currLevelOverlappings.size());
//        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());
//        if (nextLevelSSTSet.isEmpty() || currLevelOverlappings.isEmpty()) {
//            return currLevelOverlappings;
//        }
//
//        var wideRange = getSSTKeyRange(currLevelOverlappings);
//        var nextLevelOverlappings = getTransitivelyOverlapping(nextLevelSSTSet, wideRange);
//        currLevelOverlappings.addAll(nextLevelOverlappings);
//        System.out.println("Level " + level.nextLevel() + " has " + nextLevelSSTSet.size() + " files and overlapping files are " + nextLevelOverlappings.size());
        return currLevelOverlappings;
    }

    private Collection<SSTInfo> getCurrAndNextLevelOverlappins2(Level level) {
        System.out.println(level + " start getCurrAndNextLevelOverlappins2" + Thread.currentThread().getName());
        /*
        we can actually use only the ssts which has low dependecy and their recursive dependecies.
         */
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());
        System.out.println(level + " currentLevelSSTSet size is " + currentLevelSSTSet.size() + Thread.currentThread().getName());
        System.out.println(level + " nextLevelSSTSet size is " + nextLevelSSTSet.size() + Thread.currentThread().getName());
        if (currentLevelSSTSet.isEmpty()) {
            System.out.println(level + " currentLevelSSTSet is empty" + Thread.currentThread().getName());
            return Collections.emptyList();
        }

        for (SSTInfo sstInfo : currentLevelSSTSet.reversed()) {
            System.out.println(level + " getAllOverlappingFilesToCompactWithGivenRange" + Thread.currentThread().getName());
            var set = getAllOverlappingFilesToCompactWithGivenRange(level, sstInfo.getSstKeyRange(), currentLevelSSTSet, nextLevelSSTSet);
            if (!set.isEmpty()) {
                System.out.println(level + " set size is " + set.size() + Thread.currentThread().getName());
                return set;
            }
        }
        System.out.println(level + " currentLevelSSTSet is empty" + Thread.currentThread().getName());
        return List.of();
    }

    private SortedSet<SSTInfo> getAllOverlappingFilesToCompactWithGivenRange(Level level, SSTKeyRange range,  SortedSet<SSTInfo> currentLevelSSTSet, SortedSet<SSTInfo> nextLevelSSTSet) {
        /***
         * todo we can actually get all the depenmdent files, we can actually have limited number of files to be compacted per level, and based on that we can choose which depenedcy links to include
         *
         */
        var greatestKey = range.getGreatest();
        var smallestKey = range.getSmallest();
        SortedSet<SSTInfo> overlappingFiles = new TreeSet<>();
        addFilesContainingKey(overlappingFiles, greatestKey, currentLevelSSTSet);
        addFilesContainingKey(overlappingFiles, smallestKey, currentLevelSSTSet);

        Queue<SSTInfo> queue = new LinkedList<>(overlappingFiles);
        SortedSet<SSTInfo> real = new TreeSet<>();
        while (!queue.isEmpty()) {
            var sst = queue.poll();
            if (real.contains(sst)) {
                continue;
            }
            for (SSTInfo sstInfo : currentLevelSSTSet) {
                if (sstInfo.compareTo(sst) <= 0) {
                    continue;
                }
                if (sstInfo.getSstKeyRange().inRange(sst.getSstKeyRange().getSmallest()) ||
                        sstInfo.getSstKeyRange().inRange(sst.getSstKeyRange().getGreatest())) {
                    // todo this could lead to taking whole level ssts to compact.
                    queue.add(sstInfo);
                }
            }
            real.add(sst);
//            System.out.println("File " + sst.getSstPath().getName() + " has " + list.size() + " dependency of files");
        }
        if (real.isEmpty()) {
            return real;
        }
        var wideRange = getSSTKeyRange(real);
        real.addAll(getZeroDependentSSTs(nextLevelSSTSet, wideRange));

        System.out.println("Level " + level + " has " + currentLevelSSTSet.size() + " files and overlapping files are " + real.size());
        return real;
    }

    private Collection<? extends SSTInfo> getZeroDependentSSTs(SortedSet<SSTInfo> nextLevelSSTSet, SSTKeyRange wideRange) {
        SortedSet<SSTInfo> overlappingFiles = new TreeSet<>();
        for (SSTInfo original : nextLevelSSTSet) {
            if (original.getSstKeyRange().inRange(wideRange.getSmallest()) ||
                    original.getSstKeyRange().inRange(wideRange.getGreatest())) {
                boolean doesNotHaveDependency = true;
                for (SSTInfo toCompare : nextLevelSSTSet) {
                    if (toCompare.equals(original)) {
                        continue;
                    }
                    if (toCompare.getSstKeyRange().inRange(original.getSstKeyRange().getSmallest()) ||
                            toCompare.getSstKeyRange().inRange(original.getSstKeyRange().getGreatest())) {
                        doesNotHaveDependency = false;
                        break;
                    }
                }
                if(doesNotHaveDependency) {
                    overlappingFiles.add(original);
                }
            }
        }
        return overlappingFiles;
    }

    private SortedSet<SSTInfo> getTransitivelyOverlapping(SortedSet<SSTInfo> sstInfos, SSTKeyRange baseRange) {
        if (sstInfos.isEmpty()) return new TreeSet<>();
        byte[] low = baseRange.getSmallest();
        byte[] high = baseRange.getGreatest();

        SortedSet<SSTInfo> result = new TreeSet<>();
        boolean expanded;

        do {
            expanded = false;
            for (SSTInfo sst : sstInfos) {
                if (result.contains(sst)) continue;
                SSTKeyRange r = sst.getSstKeyRange();
                if (Arrays.compare(r.getSmallest(), high) <= 0 &&
                        Arrays.compare(r.getGreatest(), low) >= 0) {
                    result.add(sst);
                    if (Arrays.compare(r.getSmallest(), low) < 0) {
                        low = r.getSmallest();
                        expanded = true;
                    }
                    if (Arrays.compare(r.getGreatest(), high) > 0) {
                        high = r.getGreatest();
                        expanded = true;
                    }
                }
            }
        } while (expanded);

        return result;
    }

    private Collection<SSTInfo> getOverlappingSSTs(Level level) {
        SortedSet<SSTInfo> currentLevelSSTSet = table.getSSTInfoSet(level);
        SortedSet<SSTInfo> nextLevelSSTSet = table.getSSTInfoSet(level.nextLevel());
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
                findOverlapping(smallestKey, currentLevelSet, nextLevelSet);
                System.out.println("took greatest key");
                return overlapping;
            }

            overlapping = findOverlapping(smallestKey, currentLevelSet, nextLevelSet);
            if (overlapping.size() > 1) {
                System.out.println("took smallest key");
                return overlapping;
            }
        }
        return Collections.emptyList();
    }

    private Collection<SSTInfo> findOverlapping(byte[] common, SortedSet<SSTInfo> currentLevelSet, SortedSet<SSTInfo> nextLevelSet) {
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

    private Level performCompaction(Level level, Collection<SSTInfo> overlappingFiles) {
        System.out.println(level + " org.g2n.atomdb.Compaction Started " + Thread.currentThread().getName());
        numberOfActuallyCompactions.addAndGet(1);
        long start = System.nanoTime();
        try {
            var iterator = new MergedClusterIterator(Collections.unmodifiableCollection(overlappingFiles));
            sstPersist.writeManyFiles(level.nextLevel(), iterator, getAverageNumOfEntriesInSST(overlappingFiles));
            for (SSTInfo overlappingFile : overlappingFiles) {
                table.removeSST(overlappingFile);
            }
        } catch (Exception e) {
            logger.error("Error during compaction for level {}: {}", level, e.getMessage());
            e.printStackTrace();
            System.exit(123);
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
        executors.shutdown();
        executors.close();
        System.out.println("Number of actually compactions: " + numberOfActuallyCompactions.get());
        // number of files in each levels
        for (Level level : Level.values()) {
            System.out.println(level + " has " + table.getCurrentLevelSize(level) + " size in bytes");
            System.out.println(level + " has " + table.getSSTInfoSet(level).size() + " files");
        }
    }
}
