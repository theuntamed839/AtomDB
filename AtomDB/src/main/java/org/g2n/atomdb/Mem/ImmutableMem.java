package org.g2n.atomdb.Mem;

import org.g2n.atomdb.db.KVUnit;

import java.util.Iterator;

public interface ImmutableMem<K, V> extends Memtable<K, V>{
    static ImmutableMem<byte[], KVUnit> of(MutableMem<byte[], KVUnit> memtable) {
        return new ImmutableMemTable(memtable.getReadOnlyMap(), memtable.getMemTableSize());
    }
    Iterator<KVUnit> getKeySetIterator();
}
