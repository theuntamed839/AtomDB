package org.g2n.atomdb.Compaction;

import com.google.common.hash.BloomFilter;
import org.g2n.atomdb.db.KVUnit;

import java.util.Iterator;
import java.util.Objects;

public class ClusterFilterAddingIterator implements Iterator<IndexedCluster> {
    private final CollectiveIndexedClusterIterator iterator;
    private final BloomFilter<byte[]> filter;
    private int totalKVSize = 0;
    private int count = 0;

    public ClusterFilterAddingIterator(CollectiveIndexedClusterIterator iterator, BloomFilter<byte[]> filter) {
        this.iterator = Objects.requireNonNull(iterator, "Iterator cannot be null");
        this.filter = Objects.requireNonNull(filter, "BloomFilter cannot be null");
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public IndexedCluster next() {
        var next = iterator.next();
        for (KVUnit kvUnit : next.getEntries()) {
            filter.put(kvUnit.getKey());
            totalKVSize += kvUnit.getUnitSize();
            count++;
        }
        return next;
    }

    public int getTotalKVSize() {return totalKVSize;}
    public int getCount() {
        return count;
    }
}
