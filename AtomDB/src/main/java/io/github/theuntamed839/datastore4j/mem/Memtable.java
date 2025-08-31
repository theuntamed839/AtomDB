package io.github.theuntamed839.datastore4j.mem;

import io.github.theuntamed839.datastore4j.db.KVUnit;

public interface Memtable<K, V> {
    KVUnit get(byte[] key);

    long getMemTableSize();

    int getNumberOfEntries();

    boolean isFull();
}
