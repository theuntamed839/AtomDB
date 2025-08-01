package org.g2n.atomdb.mem;

import com.google.common.base.Preconditions;
import org.g2n.atomdb.db.KVUnit;

import java.util.Iterator;
import java.util.Objects;
import java.util.SortedMap;

public class ImmutableMemTable implements ImmutableMem<byte[], KVUnit> {
    private final SortedMap<byte[], KVUnit> map;
    private final long totalMemSize;

    public ImmutableMemTable(SortedMap<byte[], KVUnit> unmodifiableSortedMap, long totalMemSize) {
        Objects.requireNonNull(unmodifiableSortedMap, "Unmodifiable sorted map cannot be null");
        Preconditions.checkArgument(totalMemSize >= 0, "Total size must be non-negative");

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
    public boolean isFull() {
        throw new UnsupportedOperationException("ImmutableMemTable does not support isFull operation");
    }

    @Override
    public Iterator<KVUnit> getKeySetIterator() {
        return map.values().iterator();
    }
}
