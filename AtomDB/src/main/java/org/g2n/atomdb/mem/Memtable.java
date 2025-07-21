package org.g2n.atomdb.mem;

import org.g2n.atomdb.db.KVUnit;

public interface Memtable<K, V> {
    KVUnit get(byte[] key);

    long getMemTableSize();

    int getNumberOfEntries();

    boolean isFull();
}
