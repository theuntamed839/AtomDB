package Compaction;

import Constants.DBConstant;
import Level.Level;
import Mem.ImmutableMem;
import Table.Table;
import db.KVUnit;
import sstIo.MemTableBackedSSTReader;

import java.io.File;
import java.util.List;

public class Compactor {

    private final Table table;

    public Compactor(File dbFolder) {
        this.table = new Table(dbFolder);
    }

    public void persistLevelFile(ImmutableMem<byte[], KVUnit> memtable) {
        MemTableBackedSSTReader sstReader = new MemTableBackedSSTReader(memtable);
        String newSST = table.getNewSST(Level.LEVEL_ZERO);
        long start, end;
        start = System.nanoTime();
        List<Pointer> checkPoints = new SSTPersist(new File(newSST),
                sstReader.getIterator(), sstReader.getKeyRange(),
                sstReader.getEntries(), DBConstant.CLUSTER_SIZE).getCheckPoints();
        end = System.nanoTime();
        System.out.println("took="+(end - start));
    }
}
