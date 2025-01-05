package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.util.*;


public class CollectiveIndexedClusterIterator implements Iterator<KVUnit>, AutoCloseable {
    private final List<ClusterFromIndexedClusterIterator> iterators;
    private final ArrayList<byte[]> validatorList;
    private final int totalEntriesCount;
    private int totalKVSizeGivenOut = 0;
    private int numberOfEntriesGivenOut = 0;

    public CollectiveIndexedClusterIterator(Collection<SSTInfo> collection) throws IOException {
        this.iterators = new ArrayList<>(collection.size());
        this.totalEntriesCount = collection.stream().mapToInt(SSTInfo::getNumberOfEntries).sum();
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
        this.validatorList = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public KVUnit next() {
        if (!hasNext()) {
            System.out.println("No more elements CollectiveIndexedClusterIterator");
            throw new NoSuchElementException();
        }
        return nextKVUnit();
    }

    private KVUnit nextKVUnit() {
        KVUnit unit = iterators.getFirst().getNextKVUnit();
        ClusterFromIndexedClusterIterator curr = iterators.getFirst();
        var toRemove = new ArrayList<ClusterFromIndexedClusterIterator>();
        for (ClusterFromIndexedClusterIterator iterator : iterators) {
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
                    curr.pollNextKVUnit(); // old file value
                    unit = iterator.getNextKVUnit();
                    curr = iterator;
                } else {
                    iterator.pollNextKVUnit(); // old file value
                }
            }
        }
        iterators.removeAll(toRemove);
        KVUnit kvUnit = curr.pollNextKVUnit();
        if (!curr.hasNext()) {
            curr.close();
            iterators.remove(curr);
        }
        totalKVSizeGivenOut += kvUnit.getUnitSize();
        numberOfEntriesGivenOut++;
        return kvUnit;
    }

    @Override
    public void close() throws Exception {
        if (!iterators.isEmpty()) {
            throw new RuntimeException("All iterator should have been used");
        }
//        if (totalEntriesCount != validatorList.size()) {
//            throw new RuntimeException("Total entries numberOfEntriesGivenOut mismatch, expected=" + totalEntriesCount + " but got=" + validatorList.size());
//        }
    }

    public double approxRemainingNumOfEntries() {
        return totalEntriesCount - numberOfEntriesGivenOut;
    }
}