package Mem;

import db.KVUnit;

public interface Memtable<K, V> {
    KVUnit get(byte[] key);

    long getMemTableSize();

    int getNumberOfEntries();

    K getFirstKey();

    K getLastKey();

    boolean isFull();
}
