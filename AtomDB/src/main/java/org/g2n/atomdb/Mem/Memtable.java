package org.g2n.atomdb.Mem;

import org.g2n.atomdb.db.KVUnit;

public interface Memtable<K, V> {
    KVUnit get(byte[] key);

    long getMemTableSize();

    int getNumberOfEntries();

    K getFirstKey();

    K getLastKey();

    boolean isFull();
}
