package org.g2n.atomdb.mem;

import com.google.common.base.Preconditions;
import org.g2n.atomdb.db.KVUnit;

import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListMemtable implements MutableMem<byte[], KVUnit>{
    private final ConcurrentSkipListMap<byte[], KVUnit> map;
    private final int maxSize;
    private int currentSize;

    public SkipListMemtable(int maxSize, Comparator<byte[]> comparator) {
        Preconditions.checkArgument(maxSize > 0, "Max size must be greater than 0");

        this.maxSize = maxSize;
        map = new ConcurrentSkipListMap<>(comparator);
        currentSize = 0;
    }

    @Override
    public void put(KVUnit kvUnit) {
        Objects.requireNonNull(kvUnit, "KVUnit cannot be null");
        KVUnit alreadyExisting = map.put(kvUnit.getKey(), kvUnit);
        if (alreadyExisting != null) {
            currentSize -= alreadyExisting.getUnitSize();
        }
        currentSize += kvUnit.getUnitSize();
    }

    @Override
    public void delete(KVUnit kvUnit) {
        Objects.requireNonNull(kvUnit, "KVUnit cannot be null");
        if (!kvUnit.isDeleted()) {
            throw new IllegalArgumentException("Cannot delete a non-deleted KVUnit");
        }
        put(kvUnit);
    }

    @Override
    public KVUnit get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
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
