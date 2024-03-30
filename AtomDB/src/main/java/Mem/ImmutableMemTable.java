package Mem;

import db.KVUnit;

import java.util.Iterator;
import java.util.SortedMap;

public class ImmutableMemTable implements ImmutableMem<byte[], KVUnit> {
    private final SortedMap<byte[], KVUnit> map;
    private final long totalMemSize;

    public ImmutableMemTable(SortedMap<byte[], KVUnit> unmodifiableSortedMap, long totalMemSize) {
        this.map = unmodifiableSortedMap;
        this.totalMemSize = totalMemSize;
    }

    @Override
    public KVUnit get(byte[] key) {
        return map.get(key);
    }

    @Override
    public long getMemTableSize() {
        return totalMemSize;
    }

    @Override
    public int getNumberOfEntries() {
        return map.size();
    }

    @Override
    public byte[] getFirstKey() {
        return map.firstKey();
    }

    @Override
    public byte[] getLastKey() {
        return map.lastKey();
    }

    @Override
    public Iterator<KVUnit> getKeySetIterator() {
        return map.values().iterator();
    }
}
