package Mem;

import db.KVUnit;

import java.util.Iterator;
import java.util.Map;

public interface ImmutableMem<K, V> extends Memtable<K, V>{
    static ImmutableMem<byte[], KVUnit> of(MutableMem<byte[], KVUnit> memtable) {
        return new ImmutableMemTable(memtable.getReadOnlyMap(), memtable.getMemTableSize());
    }
    Iterator<Map.Entry<byte[], KVUnit>> getKeySetIterator();
}
