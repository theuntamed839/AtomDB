package Mem;

import db.KVUnit;

public interface ImmutableMem<K, V> extends Memtable<K, V>{
    static ImmutableMem<byte[], KVUnit> of(MutableMem<byte[], KVUnit> memtable) {
        return new ImmutableMemTable(memtable.getReadOnlyMap(), memtable.getMemTableSize());
    }
}
