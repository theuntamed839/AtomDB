package io.github.theuntamed839.datastore4j.mem;

import io.github.theuntamed839.datastore4j.db.KVUnit;

import java.util.Iterator;
import java.util.Set;

public interface ImmutableMem<K, V> extends Memtable<K, V>{
    static ImmutableMem<byte[], KVUnit> of(MutableMem<byte[], KVUnit> memtable) {
        return new ImmutableMemTable(memtable.getReadOnlyMap(), memtable.getMemTableSize());
    }
    Iterator<KVUnit> getValuesIterator();

    Set<byte[]> getkeySet();
}
