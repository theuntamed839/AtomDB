package Compaction;

import Constants.DBConstant;
import Level.Level;
import Mem.ImmutableMem;
import Table.Table;
import Table.SSTInfo;
import db.KVUnit;
import sstIo.MemTableBackedSSTReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

/**
 * 1. select the more who is sparse (large-small) > others and has less entries.
 * 2. overlap, Sk and LK falls other false falls in another single sst. basically the other sst contains this sst.
 * 3. choose the one which has many deleted entries. ( we have count the number of deleted entries in sst and store in the header.)
 * 4. we can have a hit count for sst, which can tell us how optimized the sst is. if more success hits then we might not consider
 *  the file for compaction and choose the one with less hit success. (hit success is finding and getting data)
 * 5. always take from oldest to newest. ( basically find sst based on above and use the oldest to newest sort)
 * 6. add the algorithm which tells how many elements can be present inside 2 key ranges.
 */
public class Compactor {

    private final Table table;

    public Compactor(Table table) {
        this.table = table;
    }

    public void persistLevelFile(ImmutableMem<byte[], KVUnit> memtable) throws IOException {
        MemTableBackedSSTReader sstReader = new MemTableBackedSSTReader(memtable);
        long start, end;
        start = System.nanoTime();
        new SSTPersist(Level.LEVEL_ZERO, sstReader.getIterator(), sstReader.getEntries(),
                DBConstant.CLUSTER_SIZE, table).write();
        end = System.nanoTime();
        System.out.println("took="+(end - start));
    }

    public void tryCompaction(Level level) throws IOException {
        int size = table.getCurrentLevelSize(level);
        if (size <= level.limitingSize()) {
            return;
        }
        System.out.println("comparator for"+level);
        SortedSet<SSTInfo> levelFileList = table.getLevelFileList(level);
        byte[] arr = table.getLastCompactedKey(level);
        List<SSTInfo> files = getFilesContainingKey(arr, levelFileList);
        if (files.isEmpty()){
            files.add(levelFileList.getFirst());
        }
         // todo here we can take the big range for level zero rather then only the range of first. need analysis
        List<SSTInfo> overlappingFiles = getOverlappingFiles(files.getFirst(), table.getLevelFileList(level.next()));

//        if (level == Level.LEVEL_ZERO) {
//            overlappingFiles.addAll(files);
//        } else {
//            overlappingFiles.add(files.getFirst());
//        }

        overlappingFiles.addAll(files);
        if (overlappingFiles.size() <= 1) {
            return;
        }
        var iterator = new CollectiveSStIterator(Collections.unmodifiableList(overlappingFiles));

        long start, end;
        start = System.nanoTime();
        new SSTPersist(level.next(), iterator,
                DBConstant.CLUSTER_SIZE, table).writeBigLevel();
        end = System.nanoTime();
        System.out.println("comparator took="+(end - start));
        System.out.println("compacted files");
        overlappingFiles.forEach(each -> System.out.println(each.getSst().getName()));
        overlappingFiles.forEach(table::removeSST);
        if (Level.LEVEL_SEVEN != level) {
            tryCompaction(level.next());
        }
    }

    private List<SSTInfo> getOverlappingFiles(SSTInfo first, SortedSet<SSTInfo> levelFileList) {
        var list = new ArrayList<SSTInfo>();
        for (SSTInfo sstInfo : levelFileList) {
            if (first.getSstKeyRange().overLapping(sstInfo.getSstKeyRange())) {
                list.add(sstInfo);
            }
        }
        return list;
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
}
