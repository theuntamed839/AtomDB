package Compaction;

import Constants.DBConstant;
import Level.Level;
import Mem.ImmutableMem;
import Table.Table;
import db.KVUnit;
import sstIo.MemTableBackedSSTReader;

import java.io.File;
import java.io.IOException;
import java.util.List;

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

    public Compactor(File dbFolder) {
        this.table = new Table(dbFolder);
    }

    public void persistLevelFile(ImmutableMem<byte[], KVUnit> memtable) throws IOException {
        MemTableBackedSSTReader sstReader = new MemTableBackedSSTReader(memtable);
        File newSST = table.getNewSST(Level.LEVEL_ZERO);
        long start, end;
        start = System.nanoTime();
        var checkPoints = new SSTPersist(newSST,
                sstReader.getIterator(), sstReader.getKeyRange(),
                sstReader.getEntries(), DBConstant.CLUSTER_SIZE).getCheckPoints();
        end = System.nanoTime();
        System.out.println("took="+(end - start));
    }
}
