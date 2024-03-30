package sstIo;

import Constants.DBConstant;
import Level.Level;
import Mem.ImmutableMem;
import db.KVUnit;

import java.util.Iterator;

public class MemTableBackedSSTReader {
    private final ImmutableMem<byte[], KVUnit> map;
    private final SSTKeyRange sstKeyRange;

    public MemTableBackedSSTReader(ImmutableMem<byte[], KVUnit> memtable) {
        this.map = memtable;
        this.sstKeyRange= new SSTKeyRange(memtable.getFirstKey(), memtable.getLastKey());
    }

    public Iterator<KVUnit> getIterator() {
        return map.getKeySetIterator();
    }

    public SSTKeyRange getKeyRange() {
        return sstKeyRange;
    }

    public int getEntries() {
        return map.getNumberOfEntries();
    }
}
