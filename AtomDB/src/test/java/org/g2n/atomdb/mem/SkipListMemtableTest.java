package org.g2n.atomdb.mem;
import org.g2n.atomdb.db.KVUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SkipListMemtableTest {

    @Test
    public void givenSingleKVUnit_whenPutInMemTable_thenSizeShouldMatchUnitSize() {
        KVUnit unit = new KVUnit("key".getBytes(), "value".getBytes());

        var mem = new SkipListMemtable(1000, Arrays::compare);
        mem.put(unit);

        assertEquals(unit.getUnitSize(), mem.getMemTableSize());
    }

    @Test
    public void givenMultipleKVUnit_whenPutInMemTable_thenSizeShouldMatchUnitSize() {
        KVUnit unit1 = new KVUnit("key1".getBytes(), "value1".getBytes());
        KVUnit unit2 = new KVUnit("key2".getBytes(), "value2".getBytes());
        KVUnit unit3 = new KVUnit("key3".getBytes(), "value3".getBytes());
        var mem = new SkipListMemtable(1000, Arrays::compare);

        mem.put(unit1);
        mem.put(unit2);
        mem.put(unit3);

        assertEquals(unit1.getUnitSize() + unit2.getUnitSize() + unit3.getUnitSize(), mem.getMemTableSize());
    }

    @Test
    public void givenMultipleKVUnit_whenPutInMemTableWithSameKey_thenSizeShouldCalculatedCorrectly() {
        KVUnit unit1 = new KVUnit("key1".getBytes(), "value1".getBytes());
        KVUnit unit2 = new KVUnit("key2".getBytes(), "value2".getBytes());
        KVUnit unit3 = new KVUnit("key3".getBytes(), "value3".getBytes());

        var updatedUnit2 = new KVUnit("key2".getBytes(), "UpdatedValue2".getBytes());
        var mem = new SkipListMemtable(1000, Arrays::compare);

        mem.put(unit1);
        mem.put(unit2);
        mem.put(unit3);
        mem.put(updatedUnit2);

        assertEquals(unit1.getUnitSize() + updatedUnit2.getUnitSize() + unit3.getUnitSize(), mem.getMemTableSize());
    }

    @Test
    public void givenMemWithMultipleElement_whenDeletedElement_thenSizeShouldMatchTheSizeAlongWithDeletedKey() {
        KVUnit unit1 = new KVUnit("key1".getBytes(), "value1".getBytes());
        KVUnit unit2 = new KVUnit("key2".getBytes(), "value2".getBytes());
        KVUnit unit3 = new KVUnit("key3".getBytes(), "value3".getBytes());
        var unit2Deleted = new KVUnit("key2".getBytes());
        var mem = new SkipListMemtable(1000, Arrays::compare);

        mem.put(unit1);
        mem.put(unit2);
        mem.put(unit3);

        mem.put(unit2Deleted);

        assertEquals(unit1.getUnitSize() + unit2Deleted.getUnitSize() + unit3.getUnitSize(), mem.getMemTableSize());
    }

    @Test
    public void givenEmptyMem_whenDeletedElement_thenSizeShouldMatchTheSizeOfDeletedKey() {
        var unit2Deleted = new KVUnit("key2".getBytes());
        var mem = new SkipListMemtable(1000, Arrays::compare);

        mem.put(unit2Deleted);

        assertEquals(unit2Deleted.getUnitSize() , mem.getMemTableSize());
    }

    @Test
    public void givenMemWithMultipleElement_whenDifferentElementDeleted_thenSizeShouldMatchTheSizeAlongWithDeletedKey() {
        KVUnit unit1 = new KVUnit("key1".getBytes(), "value1".getBytes());
        KVUnit unit2 = new KVUnit("key2".getBytes(), "value2".getBytes());
        KVUnit unit3 = new KVUnit("key3".getBytes(), "value3".getBytes());
        var deleted1 = new KVUnit("delete1".getBytes());
        var deleted2 = new KVUnit("delete2".getBytes());
        var mem = new SkipListMemtable(1000, Arrays::compare);
        mem.put(unit1);
        mem.put(unit2);
        mem.put(unit3);

        mem.put(deleted1);
        mem.put(deleted2);

        assertEquals(unit1.getUnitSize() + unit2.getUnitSize() + unit3.getUnitSize() + deleted1.getUnitSize() + deleted2.getUnitSize(), mem.getMemTableSize());
    }

    @Test
    public void givenMemWithMultipleElement_whenGetOnMem_thenItShouldReturnTheKVUnitForThatKey() {
        KVUnit unit1 = new KVUnit("key1".getBytes(), "value1".getBytes());
        KVUnit unit2 = new KVUnit("key2".getBytes(), "value2".getBytes());
        KVUnit unit3 = new KVUnit("key3".getBytes(), "value3".getBytes());

        var mem = new SkipListMemtable(1000, Arrays::compare);
        mem.put(unit1);
        mem.put(unit2);
        mem.put(unit3);
        assertEquals(unit1, mem.get(unit1.getKey()));
    }

    @Test
    public void givenFullMem_whenPutMore_thenItShouldOverFlow() {
        KVUnit unit1 = new KVUnit("key1".getBytes(), "value1".getBytes());
        var mem = new SkipListMemtable(unit1.getUnitSize(), Arrays::compare);

        mem.put(new KVUnit("key2".getBytes(), "value2".getBytes()));
        Assertions.assertTrue(mem.isFull());
    }

    @Test
    public void givenMem_whenGetReadOnlyMap_thenItShouldAReadOnlyMap() {
        var mem = new SkipListMemtable(1000, Arrays::compare);
        var map = mem.getReadOnlyMap();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> map.put("key".getBytes(), new KVUnit("value".getBytes())));
    }

    @Test
    public void stressTestConcurrentPutAndSize() throws Exception {
        int threadCount = Runtime.getRuntime().availableProcessors();
        long opsPerThread = 100_000;
        var store = new SkipListMemtable(opsPerThread * threadCount, Arrays::compare);
        Random rand = new Random(123);

        var executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    var key = new byte[rand.nextInt(50)];
                    var value = new byte[rand.nextInt(50)];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    store.put(new KVUnit(key, value));
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        executor.close();

        long expected = store.getReadOnlyMap().values().stream()
                .mapToLong(KVUnit::getUnitSize)
                .sum();

        assertEquals(expected, store.getMemTableSize(),
                "currentSize should equal sum of sizes in map after all updates");
    }
}
