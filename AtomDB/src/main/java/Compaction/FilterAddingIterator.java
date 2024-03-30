package Compaction;

import com.google.common.hash.BloomFilter;
import db.KVUnit;

import java.util.Iterator;

public class FilterAddingIterator implements Iterator<KVUnit> {
    private final Iterator<KVUnit> iterator;
    private final BloomFilter<byte[]> filter;
    private int count;

    public FilterAddingIterator(Iterator<KVUnit> iterator, BloomFilter<byte[]> filter) {

        this.iterator = iterator;
        this.filter = filter;
        this.count = 0;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KVUnit next() {
        var next = iterator.next();
        filter.put(next.getKey());
        count++;
        return next;
    }

    public int getCount() {
        return count;
    }
}
