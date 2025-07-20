package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.search.Search;

import java.io.IOException;
import java.util.*;

/*
Some assumptions this class makes:
1. the sstInfoCollection should have all the overlapping SSTs, meaning same key can't be present in any sst in any levels of the sstInfoCollection coverage (for deletion logic)

 */

public class MergedClusterIterator implements Iterator<KVUnit>, AutoCloseable {
    private final List<IndexedClusterIterator> clusterIterators;
    private final int totalEntryCount;
    private final Search search;
    private final DbComponentProvider dbComponentProvider;
    private final Collection<SSTInfo> originalCollection;
    private final Level lowestLevel;
    private int entriesServed = 0;
    private KVUnit next;

    public MergedClusterIterator(Collection<SSTInfo> sstInfoCollection, Search search, DbComponentProvider dbComponentProvider) throws IOException {
        this.search = search;
        this.dbComponentProvider = dbComponentProvider;
        Objects.requireNonNull(sstInfoCollection, "SSTInfo collection cannot be null");
        this.totalEntryCount = sstInfoCollection.stream().mapToInt(SSTInfo::getNumberOfEntries).sum();
        this.clusterIterators = initializeIterators(sstInfoCollection);
        this.originalCollection = sstInfoCollection;
        this.lowestLevel = Level.fromID(sstInfoCollection.stream()
                .map(SSTInfo::getLevel)
                .map(Level::value)
                .min(Comparator.naturalOrder())
                .orElseThrow(() -> new IllegalArgumentException("No SSTInfo provided")));
        generateNextKV();
    }

    private List<IndexedClusterIterator> initializeIterators(Collection<SSTInfo> sstInfoCollection) throws IOException {
        List<IndexedClusterIterator> iterators = new ArrayList<>(sstInfoCollection.size());
        for (SSTInfo sstInfo : sstInfoCollection) {
            iterators.add(new IndexedClusterIterator(sstInfo, dbComponentProvider));
        }

        iterators.sort(Comparator.comparing(IndexedClusterIterator::getSSTInfo, SSTInfo::compareTo)); // will keep the latest sst at front.
        return iterators;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public KVUnit next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more KV units available");
        }
        KVUnit current = next;
        try {
            generateNextKV();
        } catch (IOException e) {
            throw new RuntimeException("Error while generating next KV unit", e);
        }
        return current;
    }

    private void generateNextKV() throws IOException {
        for(; !clusterIterators.isEmpty() ;) {
            KVUnit kvUnit = fetchNextKVUnit();
            if (kvUnit.isDeleted() && isKeyNotFoundInFurtherLevels(kvUnit.getKey())) {
                continue;
            }
            this.next = kvUnit;
            return;
        }
        this.next = null;
    }

    private boolean isKeyNotFoundInFurtherLevels(byte[] key) throws IOException {
        var ssts = search.getAllSSTsWithKey(key, lowestLevel);
        ssts.removeAll(originalCollection);
        return ssts.isEmpty(); // no further levels contain the key
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

            KVUnit potentialUnit = iterator.getNextKVUnit();
            int compare = DBComparator.byteArrayComparator.compare(potentialUnit.getKey(), unit.getKey());

            if (compare <= -1) {
                unit = potentialUnit;
                curr = iterator;
            }

            if (compare == 0) {
                iterator.pollNextKVUnit();
            }
        }

        KVUnit kvUnit = curr.pollNextKVUnit();
        entriesServed++;

        if (!curr.hasNext()) {
            curr.close();
            toRemove.add(curr);
        }
        clusterIterators.removeAll(toRemove);
        return kvUnit;
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