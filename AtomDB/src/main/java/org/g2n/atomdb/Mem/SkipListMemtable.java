package org.g2n.atomdb.Mem;

import org.g2n.atomdb.db.KVUnit;

import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListMemtable implements MutableMem<byte[], KVUnit>{
    private final ConcurrentSkipListMap<byte[], KVUnit> map;
    private final int maxSize;
    private int currentSize;

    public SkipListMemtable(int maxSize, Comparator<byte[]> comparator) {
        this.maxSize = maxSize;
        map = new ConcurrentSkipListMap<>(comparator);
        currentSize = 0;
    }

    @Override
    public void put(KVUnit kvUnit) {
        map.put(kvUnit.getKey(), kvUnit);
        currentSize += kvUnit.getUnitSize();
    }

    @Override
    public void delete(KVUnit kvUnit) {
        put(kvUnit);
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
    public boolean isFull() {
        return currentSize >= maxSize;
    }

    @Override
    public SortedMap<byte[], KVUnit> getReadOnlyMap() {
        return Collections.unmodifiableSortedMap(map);
    }
}
