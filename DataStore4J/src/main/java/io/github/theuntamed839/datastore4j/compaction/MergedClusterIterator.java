package io.github.theuntamed839.datastore4j.compaction;

import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.table.SSTInfo;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import io.github.theuntamed839.datastore4j.search.Search;

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

    public MergedClusterIterator(Collection<SSTInfo> sstInfoCollection, Search search, DbComponentProvider dbComponentProvider) throws Exception {
        this.search = search;
        this.dbComponentProvider = dbComponentProvider;
        Objects.requireNonNull(sstInfoCollection, "SSTInfo collection cannot be null");
        this.totalEntryCount = sstInfoCollection.stream().mapToInt(SSTInfo::getNumberOfEntries).sum();
        this.clusterIterators = initializeIterators(sstInfoCollection);
        this.originalCollection = sstInfoCollection;
        this.lowestLevel = Level.of(sstInfoCollection.stream()
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
        } catch (Exception e) {
            throw new RuntimeException("Error while generating next KV unit", e);
        }
        return current;
    }

    private void generateNextKV() throws Exception {
        for(; !clusterIterators.isEmpty() ;) {
            KVUnit kvUnit = fetchNextKVUnit();
            if (kvUnit == null || (kvUnit.isTombStone() && isKeyNotFoundInFurtherLevels(kvUnit.getKey()))) {
                continue;
            }
            this.next = kvUnit;
            return;
        }
        this.next = null;
    }

    private boolean isKeyNotFoundInFurtherLevels(byte[] key) throws Exception {
        var ssts = search.getAllSSTsWithKey(key, lowestLevel);
        ssts.removeAll(originalCollection);
        return ssts.isEmpty(); // no further levels contain the key
    }

    private KVUnit fetchNextKVUnit() throws IOException {
        KVUnit currUnit = null;
        IndexedClusterIterator curr = null;
        var toRemove = new ArrayList<IndexedClusterIterator>();
        for (IndexedClusterIterator iterator : clusterIterators) {
            if (!iterator.hasNext()) {
                iterator.close();
                toRemove.add(iterator);
                continue;
            }

            KVUnit potentialUnit = iterator.getNextKVUnit();
            if (currUnit == null) {
                currUnit = potentialUnit;
                curr = iterator;
                continue;
            }

            int compare = dbComponentProvider.getComparator().compare(potentialUnit.getKey(), currUnit.getKey());

            if (compare <= -1) {
                currUnit = potentialUnit;
                curr = iterator;
            }

            if (compare == 0) {
                iterator.pollNextKVUnit();
            }
        }

        entriesServed++;

        if (curr == null) {
            clusterIterators.removeAll(toRemove);
            return null; // no more KV units available
        }

        KVUnit kvUnit = curr.pollNextKVUnit();
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