package Mem;

import db.KVUnit;

import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListMemtable implements MutableMem<byte[], KVUnit>{
    public final Comparator<byte[]> comparator;
    private final ConcurrentSkipListMap<byte[], KVUnit> map;
    private int currentSize;

    public SkipListMemtable(Comparator<byte[]> comparator) {
        this.comparator = comparator;
        map = new ConcurrentSkipListMap<>(comparator);
        currentSize = 0;
    }

    @Override
    public void put(KVUnit kvUnit) {
        addToMemtable(kvUnit);
    }

    @Override
    public void delete(KVUnit kvUnit) {
        addToMemtable(kvUnit);
    }

    @Override
    public KVUnit get(byte[] key) {
        return map.get(key);
    }

    public long getMemTableSize() {
        return currentSize;
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
    public SortedMap<byte[], KVUnit> getReadOnlyMap() {
        return Collections.unmodifiableSortedMap(map);
    }

    private void addToMemtable(KVUnit kvUnit) {
        map.put(kvUnit.getKey(), kvUnit);
        currentSize += kvUnit.unitSize();
    }
}
