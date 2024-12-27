package org.g2n.atomdb.Compaction;

import com.google.common.hash.BloomFilter;
import org.g2n.atomdb.db.KVUnit;

import java.util.Iterator;
import java.util.Objects;

public class FilterAddingIterator implements Iterator<KVUnit> {
    private final Iterator<KVUnit> iterator;
    private final BloomFilter<byte[]> filter;
    private int totalKVSize = 0;
    private int count = 0;

    public FilterAddingIterator(Iterator<KVUnit> iterator, BloomFilter<byte[]> filter) {
        this.iterator = Objects.requireNonNull(iterator, "Iterator cannot be null");
        this.filter = Objects.requireNonNull(filter, "BloomFilter cannot be null");
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KVUnit next() {
        var next = iterator.next();
        filter.put(next.getKey());
        totalKVSize += next.getUnitSize();
        count++;
        return next;
    }

    public int getTotalKVSize() {return totalKVSize;}
    public int getCount() {
        return count;
    }
}
