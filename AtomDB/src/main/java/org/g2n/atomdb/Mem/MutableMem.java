package org.g2n.atomdb.Mem;

import java.util.SortedMap;

public interface MutableMem<K, V> extends Memtable<K, V>{
    void put(V kv);
    void delete(V kv);

    SortedMap<K, V> getReadOnlyMap();
}
