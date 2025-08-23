package io.github.theuntamed839.atomdb.mem;

import java.util.SortedMap;

public interface MutableMem<K, V> extends Memtable<K, V>{
    void put(V kv);
    SortedMap<K, V> getReadOnlyMap();
}
