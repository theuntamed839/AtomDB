package sstIo;

import Constants.DBConstant;
import Level.Level;
import Mem.ImmutableMem;
import db.KVUnit;

import java.util.Iterator;

public class MemTableBackedSSTReader {
    private final ImmutableMem<byte[], KVUnit> map;

    public MemTableBackedSSTReader(ImmutableMem<byte[], KVUnit> memtable) {
        this.map = memtable;
    }

    public Iterator<KVUnit> getIterator() {
        return map.getKeySetIterator();
    }

    public int getEntries() {
        return map.getNumberOfEntries();
    }
}
