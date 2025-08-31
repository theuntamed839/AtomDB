package io.github.theuntamed839.datastore4j.mem;

import com.google.common.base.Preconditions;
import io.github.theuntamed839.datastore4j.db.KVUnit;

import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class SkipListMemtable implements MutableMem<byte[], KVUnit>{
    private final ConcurrentSkipListMap<byte[], KVUnit> map;
    private final long maxSize;
    private final AtomicLong currentSize = new AtomicLong(0);

    public SkipListMemtable(long maxSize, Comparator<byte[]> comparator) {
        Preconditions.checkArgument(maxSize > 0, "Max size must be greater than 0");
        this.maxSize = maxSize;
        map = new ConcurrentSkipListMap<>(comparator);
    }

    @Override
    public void put(KVUnit kvUnit) {
        Objects.requireNonNull(kvUnit, "KVUnit cannot be null");
        KVUnit alreadyExisting = map.put(kvUnit.getKey(), kvUnit);
        if (alreadyExisting != null) {
            currentSize.addAndGet(-alreadyExisting.getUnitSize());
        }
        currentSize.addAndGet(kvUnit.getUnitSize());
    }

    @Override
    public KVUnit get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        return map.get(key);
    }

    @Override
    public long getMemTableSize() {
        return currentSize.longValue();
    }

    @Override
    public int getNumberOfEntries() {
        return map.size();
    }

    @Override
    public boolean isFull() {
        return currentSize.longValue() >= maxSize;
    }

    @Override
    public SortedMap<byte[], KVUnit> getReadOnlyMap() {
        return Collections.unmodifiableSortedMap(map);
    }
}
