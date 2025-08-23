package io.github.theuntamed839.atomdb.mem;

import io.github.theuntamed839.atomdb.db.KVUnit;

public interface Memtable<K, V> {
    KVUnit get(byte[] key);

    long getMemTableSize();

    int getNumberOfEntries();

    boolean isFull();
}
