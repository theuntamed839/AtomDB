package org.g2n.atomdb.Compaction;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.SSTIO.SSTPersist;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.Table.Table;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.DbOptions;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.search.Search;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MergedClusterIteratorTest {

    private FileSystem jimfs;
    private SSTPersist sstPersist;
    private Table table;
    private Search search;
    private DbComponentProvider dbComponentProvider;

    @AfterEach
    public void tearDown() throws Exception {
        if (search != null) {
            search.close();
        }
        if (jimfs != null) {
            jimfs.close();
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        this.jimfs = Jimfs.newFileSystem(Configuration.unix());
        var dbPath = Files.createTempDirectory(jimfs.getPath("/"), "IndexedClusterIteratorTest_" + Instant.now().toEpochMilli());
        var dbOptions = new DbOptions();
        dbOptions.disallowUseOfMMap();
        this.dbComponentProvider = new DbComponentProvider(dbOptions);
        this.table = new Table(dbPath, dbComponentProvider);
        this.sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
        this.search = new Search(table, dbComponentProvider);
    }

    @Test
    public void TestHasNext_returnsFalseAfterAllEntriesIterated() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of());
        var kvs2 = generateSortedRandomKVs(100, List.of());
        var kvs3 = generateSortedRandomKVs(100, List.of());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                this.search,
                this.dbComponentProvider
        );

        int totalEntries = kvs1.size() + kvs2.size() + kvs3.size();
        for (int i = 0; i < totalEntries; i++) {
            assertTrue(mergedClusterIterator.hasNext(), "Expected hasNext to return true for entry " + i);
            mergedClusterIterator.next();
        }
        assertFalse(mergedClusterIterator.hasNext(), "Expected hasNext to return false after all entries are iterated");
    }

    @Test
    public void TestNext_throwsExceptionAfterAllEntriesConsumed() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of());
        var kvs2 = generateSortedRandomKVs(100, List.of());
        var kvs3 = generateSortedRandomKVs(100, List.of());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                this.search,
                this.dbComponentProvider
        );

        int totalEntries = kvs1.size() + kvs2.size() + kvs3.size();
        for (int i = 0; i < totalEntries; i++) {
            mergedClusterIterator.next();
        }

        assertThrows(NoSuchElementException.class, mergedClusterIterator::next,
                "Expected NoSuchElementException when next is called after all entries are iterated");
    }


    @Test
    public void TestNext_returnsMergedSortedKVUnitsFromMultipleSSTFiles() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of());
        var kvs2 = generateSortedRandomKVs(100, List.of());
        var kvs3 = generateSortedRandomKVs(100, List.of());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                this.search,
                this.dbComponentProvider
        );

        kvs1.addAll(kvs2);
        kvs1.addAll(kvs3);

        kvs1.sort(Comparator.comparing(KVUnit::getKey, DBComparator.byteArrayComparator));

        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            KVUnit expected = kvs1.removeFirst();
            assertArrayEquals(expected.getKey(), next.getKey(), "Keys do not match");
            assertArrayEquals(expected.getValue(), next.getValue(), "Values do not match");
        }
        assertTrue(kvs1.isEmpty(), "Not all expected KV units were iterated over");
    }

    @Test
    public void TestNext_returnsLatestValueForDuplicateKeyAcrossSSTs() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value1".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value2".getBytes())));
        var kvs3 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value3".getBytes())));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                this.search,
                this.dbComponentProvider
        );

        var isAbsent = true;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key".getBytes(), next.getKey())) {
                assertArrayEquals("value3".getBytes(), next.getValue(), "Expected latest value for key");
                isAbsent = false;
            }
        }

        assertFalse(isAbsent, "Expected key 'key' to be present in the merged iterator");
    }

    // deletion
    @Test
    public void TestNext_skipsDeletedKeyIfNotPresentInLowerLevels() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of());
        var kvs3 = generateSortedRandomKVs(100, List.of());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        Search search = mock(Search.class);
        when(search.getAllSSTsWithKey(any(byte[].class), eq(Level.LEVEL_ZERO)))
                .thenReturn(new ArrayList<>());

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                search,
                this.dbComponentProvider
        );

        var isPresent = false;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key".getBytes(), next.getKey())) {
                // raise assertion error if we find the key
                isPresent = true;
                break;
            }
        }
        assertFalse(isPresent, "Expected key 'key' not to be present in the merged iterator");
    }

    @Test
    public void TextNext_retainsTombstoneIfKeyExistsInHigherLevels() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of());
        var kvs3 = generateSortedRandomKVs(100, List.of());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        Search search = mock(Search.class);
        var arr = new ArrayList<SSTInfo>();
        arr.add(mock(SSTInfo.class));
        when(search.getAllSSTsWithKey(any(byte[].class), eq(Level.LEVEL_ZERO)))
                .thenReturn(arr);

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                search,
                this.dbComponentProvider
        );

        var count = 0;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key".getBytes(), next.getKey())) {
                assertTrue(next.isDeleted(), "Expected key 'key' to be a tombstone in the merged iterator");
                count++;
                break;
            }
        }
        assertEquals(1, count, "Expected key 'key' to be present in the merged iterator as a tombstone");
    }

    @Test
    public void TextNext_removesMultipleDeletedKeysIfNotInHigherLevels() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key1".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of(new KVUnit("key2".getBytes())));
        var kvs3 = generateSortedRandomKVs(100, List.of(new KVUnit("key3".getBytes())));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        Search search = mock(Search.class);
        when(search.getAllSSTsWithKey(any(byte[].class), eq(Level.LEVEL_ZERO)))
                .thenReturn(new ArrayList<>());

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                search,
                this.dbComponentProvider
        );

        var presenceCount = 0;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key1".getBytes(), next.getKey()) ||
                Arrays.equals("key2".getBytes(), next.getKey()) ||
                Arrays.equals("key3".getBytes(), next.getKey())) {
                presenceCount++;
                break;
            }
        }
        assertEquals(0, presenceCount, "Expected no deleted  keys to be present in the merged iterator");
    }

    @Test
    public void TestNext_returnsLatestValueForDeletedKeyIfNoLowerLevel() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value2".getBytes())));
        var kvs3 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value3".getBytes())));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        Search search = mock(Search.class);
        when(search.getAllSSTsWithKey(any(byte[].class), eq(Level.LEVEL_ZERO)))
                .thenReturn(new ArrayList<>());

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                search,
                this.dbComponentProvider
        );

        int count = 0;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key".getBytes(), next.getKey())) {
                assertArrayEquals("value3".getBytes(), next.getValue(), "Expected latest value for key");
                count++;
            }
        }
        assertEquals(1, count, "Expected key 'key' to be present only once with the latest value in the merged iterator");
    }

    @Test
    public void TestNext_returnsLatestValueForDeletedKeyWithHigherLevelPresence() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value2".getBytes())));
        var kvs3 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value3".getBytes())));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        Search search = mock(Search.class);
        var arr = new ArrayList<SSTInfo>();
        arr.add(mock(SSTInfo.class));
        when(search.getAllSSTsWithKey(any(byte[].class), eq(Level.LEVEL_ZERO)))
                .thenReturn(arr);

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                search,
                this.dbComponentProvider
        );

        int count = 0;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key".getBytes(), next.getKey())) {
                assertArrayEquals("value3".getBytes(), next.getValue(), "Expected latest value for key");
                count++;
            }
        }
        assertEquals(1, count, "Expected key 'key' to be present only once with the latest value in the merged iterator");
    }

    @Test
    public void TestNext_deletesKeyBasedOnLatestSSTTombstone() throws Exception {
        var kvs1 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value1".getBytes())));
        var kvs2 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes(), "value2".getBytes())));
        var kvs3 = generateSortedRandomKVs(100, List.of(new KVUnit("key".getBytes())));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs1.size(), kvs1.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, kvs2.size(), kvs2.iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs3.size(), kvs3.iterator());

        SortedSet<SSTInfo> fileListView = table.getFileListView();
        ArrayList<SSTInfo> sstInfos = new ArrayList<>(fileListView);
        Collections.shuffle(sstInfos);

        Search search = mock(Search.class);
        when(search.getAllSSTsWithKey(any(byte[].class), eq(Level.LEVEL_ZERO)))
                .thenReturn(new ArrayList<>());

        MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(
                sstInfos,
                search,
                this.dbComponentProvider
        );

        int count = 0;
        while (mergedClusterIterator.hasNext()) {
            KVUnit next = mergedClusterIterator.next();
            if (Arrays.equals("key".getBytes(), next.getKey())) {
                count++;
            }
        }
        assertEquals(0, count, "Expected key 'key' to be deleted based on latest SST");
    }

    private List<KVUnit> generateSortedRandomKVs(int count, List<KVUnit> toInclude) {
        var rand = new Random();
        List<KVUnit> kvUnits = new ArrayList<>();
        for (int i = 0; i < count - toInclude.size(); i++) {
            byte[] key = new byte[rand.nextInt(10, 100)];
            rand.nextBytes(key);
            byte[] value = new byte[rand.nextInt(10, 100)];
            rand.nextBytes(value);
            kvUnits.add(new KVUnit(key, value));
        }
        kvUnits.addAll(toInclude);
        kvUnits.sort(Comparator.comparing(KVUnit::getKey, DBComparator.byteArrayComparator));
        return kvUnits;
    }
}