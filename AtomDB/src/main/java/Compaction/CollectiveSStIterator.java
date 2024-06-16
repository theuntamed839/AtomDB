package Compaction;

import Table.SSTInfo;
import com.google.common.collect.PeekingIterator;
import db.DBComparator;
import db.KVUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectiveSStIterator implements Iterator<KVUnit>, AutoCloseable{
    private final Collection<SSTInfo> files;
    private final List<PeekingIterator<KVUnit>> iterators;
    private PeekingIterator<KVUnit> currentUnitIterator;

    public CollectiveSStIterator(Collection<SSTInfo> collection) throws IOException {
        this.files = collection;
        this.iterators = new ArrayList<>(collection.size());
        for (SSTInfo sstInfo : collection) {
            iterators.add(new SSTIterator(sstInfo));
        }
    }

    @Override
    public boolean hasNext() {
        load();
        return currentUnitIterator != null;
    }

    @Override
    public KVUnit next() {
        load();
        KVUnit next = currentUnitIterator.next();
        currentUnitIterator = null;
        return next;
    }

    private void load() {
        if (currentUnitIterator != null) {
            return;
        }
        List<PeekingIterator<KVUnit>> toRemove = new ArrayList<>();

        currentUnitIterator = null;
        for (PeekingIterator<KVUnit> iterator : iterators) {
            if (iterator.hasNext()) {
                if (currentUnitIterator == null || DBComparator.byteArrayComparator.compare(
                        currentUnitIterator.peek().getKey(), iterator.peek().getKey()) > 0) {
                    currentUnitIterator = iterator;
                }
            } else  {
                toRemove.add(iterator);
            }
        }
        for (PeekingIterator<KVUnit> iterator : toRemove) {
            try {
                iterators.remove(iterator);
                ((SSTIterator) iterator).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (currentUnitIterator != null) {
            ((SSTIterator) currentUnitIterator).close();
        }
        for (PeekingIterator<KVUnit> iterator : iterators) {
            ((SSTIterator) iterator).close();
        }
    }
}