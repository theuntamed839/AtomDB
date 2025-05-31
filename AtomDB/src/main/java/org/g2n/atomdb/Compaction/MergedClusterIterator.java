package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.util.*;

public class MergedClusterIterator implements Iterator<KVUnit>, AutoCloseable {
    private final List<IndexedClusterIterator> clusterIterators;
    private final int totalEntryCount;
    private int entriesServed = 0;

    public MergedClusterIterator(Collection<SSTInfo> sstInfoCollection) throws IOException {
        Objects.requireNonNull(sstInfoCollection, "SSTInfo collection cannot be null");
        this.totalEntryCount = sstInfoCollection.stream().mapToInt(SSTInfo::getNumberOfEntries).sum();
        this.clusterIterators = initializeIterators(sstInfoCollection);
    }

    private List<IndexedClusterIterator> initializeIterators(Collection<SSTInfo> sstInfoCollection) throws IOException {
        List<IndexedClusterIterator> iterators = new ArrayList<>(sstInfoCollection.size());
        for (SSTInfo sstInfo : sstInfoCollection) {
            iterators.add(new IndexedClusterIterator(sstInfo));
        }
        iterators.sort((a, b) -> {
            try {
                return DBComparator.byteArrayComparator.compare(a.nextClusterSmallestKey(), b.nextClusterSmallestKey());
            } catch (Exception e) {
                throw new IllegalStateException("Error comparing cluster smallest keys", e);
            }
        });
        return iterators;
    }

    @Override
    public boolean hasNext() {
        return !clusterIterators.isEmpty();
    }

    @Override
    public KVUnit next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more KV units available");
        }
        try {
            return fetchNextKVUnit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private KVUnit fetchNextKVUnit() throws IOException {
        KVUnit unit = clusterIterators.getFirst().getNextKVUnit();
        IndexedClusterIterator curr = clusterIterators.getFirst();
        var toRemove = new ArrayList<IndexedClusterIterator>();

        for (IndexedClusterIterator iterator : clusterIterators) {
            if (!iterator.hasNext()) {
                iterator.close();
                toRemove.add(iterator);
                continue;
            }

            if (iterator.equals(curr)) {
                continue;
            }

            int compare = DBComparator.byteArrayComparator.compare(iterator.getNextKVUnit().getKey(), unit.getKey());

            if (compare <= -1) {
                unit = iterator.getNextKVUnit();
                curr = iterator;
            }

            if (compare == 0) {
                if (iterator.getSSTInfo().compareTo(curr.getSSTInfo()) < 0) {
                    curr.pollNextKVUnit(); // old path value
                    unit = iterator.getNextKVUnit();
                    curr = iterator;
                } else {
                    iterator.pollNextKVUnit(); // old path value
                }
            }
        }
        KVUnit kvUnit = curr.pollNextKVUnit();
        removeExhaustedIterator(curr, toRemove);
        entriesServed++;
        return kvUnit;
    }

    private void removeExhaustedIterator(IndexedClusterIterator curr, ArrayList<IndexedClusterIterator> toRemove) throws IOException {
        if (!curr.hasNext()) {
            curr.close();
            toRemove.add(curr);
        }
        // todo we need to remove below code
//        for (IndexedClusterIterator iterator : clusterIterators) {
//            if (!iterator.hasNext()) {
//                try{
//                    iterator.close();
//                } catch (IllegalStateException e) {
//                    // Handle the exception if needed
//                    System.err.println("Error closing iterator: " + e.getMessage());
//                }
//                toRemove.add(iterator);
//            }
//        }
        // till here
        clusterIterators.removeAll(toRemove);
    }


    public double approximateRemainingEntries() {
        return totalEntryCount - entriesServed;
    }

    @Override
    public void close() throws IOException {
        for (IndexedClusterIterator iterator : clusterIterators) {
            iterator.close();
        }
        clusterIterators.clear();
    }
}