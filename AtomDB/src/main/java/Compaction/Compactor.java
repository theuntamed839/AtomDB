package Compaction;

import Level.Level;
import Mem.ImmutableMem;
import Table.Table;
import Table.SSTInfo;
import db.DBComparator;
import db.DbOptions;
import db.KVUnit;
import sstIo.SSTKeyRange;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * 3. choose the one which has many deleted entries. ( we have count the number of deleted entries in sst and store in the header.)
 * 4. we can have a hit count for sst, which can tell us how optimized the sst is. if more success hits then we might not consider
 *  the file for compaction and choose the one with less hit success. (hit success is finding and getting data)
 * 5. always take from oldest to newest. ( basically find sst based on above and use the oldest to newest sort)
 * 6. add the algorithm which tells how many elements can be present inside 2 key ranges.
 */
public class Compactor implements AutoCloseable{
    private final Table table;
    private final DbOptions dbOptions;
    private ConcurrentMap<Level, Boolean> compactions = new ConcurrentHashMap<>();
    private ExecutorService executors = Executors.newCachedThreadPool();

    public Compactor(Table table, DbOptions dbOptions) {
        this.table = table;
        this.dbOptions = dbOptions;
    }

    public void persistLevelFile(ImmutableMem<byte[], KVUnit> memtable) throws IOException {
        SSTPersist.writeSingleFile(Level.LEVEL_ZERO, memtable.getNumberOfEntries(), memtable.getKeySetIterator(), table);
    }

    public synchronized void tryCompaction(Level level) throws Exception {
        //if (true) return;
        int size = table.getCurrentLevelSize(level);
        if (size <= level.limitingSize() || compactions.getOrDefault(level, false) || compactions.getOrDefault(level.next(), false)) {
            return;
        }

        SortedSet<SSTInfo> levelFileList = table.getLevelFileList(level);

        byte[] arr = table.getLastCompactedKey(level);
        List<SSTInfo> overlappingFiles = getFilesContainingKey(arr, levelFileList);

         // todo here we can take the big range for level zero rather tlevelhen only the range of first. need analysis
//        var nextLevelOverlappingFiles = getOverlappingFiles(overlappingFiles.getLast().getSstKeyRange(), table.getLevelFileList(level.next()));
        Collection<SSTInfo> nextLevelOverlappingFiles = getFilesContainingKey(arr, table.getLevelFileList(level.next()));
        nextLevelOverlappingFiles.addAll(overlappingFiles);

        if (nextLevelOverlappingFiles.size() <= 1) {
            nextLevelOverlappingFiles = getFilesContainingKey(levelFileList.getLast().getSstKeyRange().getSmallest(), levelFileList);

        }

        if (nextLevelOverlappingFiles.size() <= 1) {
            return;
        }

        Collection<SSTInfo> finalNextLevelOverlappingFiles = nextLevelOverlappingFiles;
        Future<?> submit = executors.submit(() -> {
            try {
                scheduleCompaction(level, finalNextLevelOverlappingFiles);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
//        scheduleCompaction(level, finalNextLevelOverlappingFiles);
    }

    private void scheduleCompaction(Level level, Collection<SSTInfo> nextLevelOverlappingFiles) throws Exception {
        long start = System.nanoTime();
        System.out.println(level + " Compaction Started " + Thread.currentThread().getName());
        compactions.put(level, true);
        var iterator = new CollectiveSStIterator(Collections.unmodifiableCollection(nextLevelOverlappingFiles), dbOptions);

        SSTPersist.writeManyFiles(level.next(), iterator, table);

        nextLevelOverlappingFiles.forEach(table::removeSST);

        compactions.put(level, false);
        if (Level.LEVEL_SEVEN != level) {
            tryCompaction(level.next());
        }
        System.out.println(level + " Compaction Ended   " + Thread.currentThread().getName() + " took=" + (System.nanoTime() - start)/1000_000_000.0 + " Seconds");
    }

    private List<SSTInfo> getFilesContainingKey(byte[] key, SortedSet<SSTInfo> levelFileList) {
        if (key == null) {
            return new ArrayList<>();
        }
        var list = new ArrayList<SSTInfo>();
        for (SSTInfo sstInfo : levelFileList) {
            if (sstInfo.getSstKeyRange().inRange(key)) {
                list.add(sstInfo);
            }
        }
        return list;
    }

    private SSTKeyRange getBigRange(List<SSTInfo> files) {
        var low = files.getFirst().getSstKeyRange().getSmallest();
        var high = files.getFirst().getSstKeyRange().getGreatest();
        for (SSTInfo file : files) {
            if (DBComparator.byteArrayComparator.compare(low, file.getSstKeyRange().getSmallest()) > 0) {
                low = file.getSstKeyRange().getSmallest();
            }
            if (DBComparator.byteArrayComparator.compare(high, file.getSstKeyRange().getGreatest()) < 0) {
                high = file.getSstKeyRange().getGreatest();
            }
        }
        return new SSTKeyRange(low, high);
    }

    private SortedSet<SSTInfo> getOverlappingFiles(SSTKeyRange range, SortedSet<SSTInfo> levelFileList) {
        var set = new TreeSet<SSTInfo>();
        for (SSTInfo sstInfo : levelFileList) {
            if (range.overLapping(sstInfo.getSstKeyRange())) {
                set.add(sstInfo);
            }
        }
        return set;
    }

    @Override
    public void close() throws Exception {
        executors.close();
        executors.shutdown();
    }
}
