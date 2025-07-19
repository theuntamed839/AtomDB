package org.g2n.atomdb.SSTIO;

import org.g2n.atomdb.db.KVUnit;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class ObservingIterator implements Iterator<KVUnit> {
    private final Iterator<KVUnit> iterator;
    private final Consumer<byte[]>  observer;

    public ObservingIterator(Iterator<KVUnit> iterator, Consumer<byte[]> observer) {
        this.iterator = iterator;
        this.observer = observer;
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public KVUnit next() {
        if (iterator.hasNext()) {
            KVUnit unit = iterator.next();
            observer.accept(unit.getKey());
            return unit;
        }
        throw new NoSuchElementException("No more elements in the iterator");
    }
}
