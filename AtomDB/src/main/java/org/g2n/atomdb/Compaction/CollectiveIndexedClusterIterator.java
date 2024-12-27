package org.g2n.atomdb.Compaction;

import com.google.common.collect.PeekingIterator;
import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.util.*;

public class CollectiveIndexedClusterIterator implements Iterator<IndexedCluster>, AutoCloseable {
    private final List<ClusterFromIndexedClusterIterator> iterators;
    private final TreeSet<KVUnit> kvs;
    private ClusterFromIndexedClusterIterator nextIteratorWithSmallestKey;

    public CollectiveIndexedClusterIterator(Collection<SSTInfo> collection) throws IOException {
        this.iterators = new ArrayList<>(collection.size());
        for (SSTInfo sstInfo : collection) {
            iterators.add(new ClusterFromIndexedClusterIterator(sstInfo));
        }
        this.iterators.sort((a, b) -> {
            try {
                return DBComparator.byteArrayComparator.compare(a.nextClusterSmallestKey(), b.nextClusterSmallestKey());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        this.kvs = new TreeSet<>((a, b) -> {
            try {
                return DBComparator.byteArrayComparator.compare(a.getKey(), b.getKey());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        this.nextIteratorWithSmallestKey = iterators.getFirst();
        load();
    }

    @Override
    public boolean hasNext() {
        if (!kvs.isEmpty()) {
            return true;
        }
        try {
            load();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return !kvs.isEmpty();
    }

    @Override
    public IndexedCluster next() {
        if (!hasNext()) {
            System.out.println("No more elements CollectiveIndexedClusterIterator");
            throw new NoSuchElementException();
        }
        return getNextCluster();
    }

    private void load() throws IOException {
        if (iterators.isEmpty()) {
            System.out.println("wait i shouldn't have been called, because hasNext is mandatory before calling me");
            return;
        }
        kvs.addAll(nextIteratorWithSmallestKey.next().getUnits());
        var listToRemove = new ArrayList<ClusterFromIndexedClusterIterator>();
        for (ClusterFromIndexedClusterIterator iterator : iterators) {
            while (iterator.hasNext() && DBComparator.byteArrayComparator.compare(iterator.nextClusterSmallestKey(), kvs.getLast().getKey()) < 0) {
                kvs.addAll(iterator.next().getUnits());
            }
            if (!iterator.hasNext()) {
                iterator.close();
                listToRemove.add(iterator);
            }
        }
        iterators.removeAll(listToRemove);
        if (iterators.isEmpty()){
            return;
        }
        nextIteratorWithSmallestKey = iterators.getFirst();
        for (ClusterFromIndexedClusterIterator iterator : iterators) {
            nextIteratorWithSmallestKey = DBComparator.byteArrayComparator.compare(iterator.nextClusterSmallestKey(), nextIteratorWithSmallestKey.nextClusterSmallestKey()) < 0 ? iterator : nextIteratorWithSmallestKey;
        }
    }

    private IndexedCluster getNextCluster() {
        var cluster = new IndexedCluster(DBConstant.CLUSTER_SIZE);
        for (int i = 0; i < DBConstant.CLUSTER_SIZE && !kvs.isEmpty(); i++) {
            cluster.add(kvs.pollFirst());
        }
        return cluster;
    }

    @Override
    public void close() throws Exception {
        iterators.forEach(iterator -> {
            try {
                iterator.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}