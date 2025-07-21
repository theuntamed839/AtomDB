package org.g2n.atomdb.mem;

import java.util.SortedMap;

public interface MutableMem<K, V> extends Memtable<K, V>{
    void put(V kv);
    void delete(V kv);

    SortedMap<K, V> getReadOnlyMap();
}
