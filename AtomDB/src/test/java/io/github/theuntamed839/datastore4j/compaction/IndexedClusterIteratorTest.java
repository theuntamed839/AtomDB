package io.github.theuntamed839.datastore4j.compaction;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.search.Search;
import io.github.theuntamed839.datastore4j.sstIO.SSTPersist;
import io.github.theuntamed839.datastore4j.table.SSTInfo;
import io.github.theuntamed839.datastore4j.table.Table;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.DbOptions;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;

class IndexedClusterIteratorTest {
    public static final int MAX_ENTRIES = 100000;
    private SSTInfo sstInfo;
    private IndexedClusterIterator indexedClusterIterator;
    private List<KVUnit> sortedKVs;
    private FileSystem jimfs;

    @BeforeEach
    public void setUp() throws Exception {
        this.jimfs = Jimfs.newFileSystem(Configuration.unix());
        var dbPath = Files.createTempDirectory(jimfs.getPath("/"), "IndexedClusterIteratorTest_" + Instant.now().toEpochMilli());
        var dbOptions = new DbOptions();
        dbOptions.disallowUseOfMMap();
        var dbComponentProvider = new DbComponentProvider(dbOptions);
        var table = new Table(dbPath,new Search(dbComponentProvider), dbComponentProvider);
        var sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
        this.sortedKVs = generateSortedRandomKVs(MAX_ENTRIES);
        sstPersist.writeSingleFile(
                Level.LEVEL_ZERO,
                MAX_ENTRIES,
                this.sortedKVs.iterator()
        );
        this.sstInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).first();
        this.indexedClusterIterator = new IndexedClusterIterator(sstInfo, dbComponentProvider);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (indexedClusterIterator != null) {
            indexedClusterIterator.close();
        }
        if (jimfs != null) {
            jimfs.close();
        }
    }

    private List<KVUnit> generateSortedRandomKVs(int count) {
        var rand = new Random();
        List<KVUnit> kvUnits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            byte[] value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            kvUnits.add(new KVUnit(key, value));
        }
        kvUnits.sort((a, b) -> Arrays.compare(a.getKey(), b.getKey()));
        return kvUnits;
    }

    @Test
    void testHasNext_returnsTrueTillLastElementRetrieved() throws IOException {
        for (int i = 0; i < MAX_ENTRIES; i++) {
            assertTrue(indexedClusterIterator.hasNext(), "Expected hasNext to return true for entry " + i);
            indexedClusterIterator.pollNextKVUnit();
        }
    }

    @Test
    void testNextClusterSmallestKey_returnsTheSmallestOfCluster() throws IOException {
        PointerList pointers = this.sstInfo.getPointers();

        for (int i = 0; i < pointers.size() - 1; i++) {
            indexedClusterIterator.nextClusterSmallestKey();
            assertEquals(indexedClusterIterator.nextClusterSmallestKey(), pointers.get(i).key(),
                    "Expected smallest key of cluster to match pointer key at index " + i);
            for (int j = 0; j < sstInfo.getSingleClusterSize(); j++) {
                indexedClusterIterator.pollNextKVUnit();
            }
        }
    }

    @Test
    void testGetNextKVUnit_returnsNextKVWithoutRemovingIt() throws IOException {
        for (KVUnit sortedKV : this.sortedKVs) {
            KVUnit nextKVUnit = indexedClusterIterator.getNextKVUnit();
            assertArrayEquals(sortedKV.getKey(), nextKVUnit.getKey(),
                    "Expected next KV unit key to match sorted key");
            assertArrayEquals(sortedKV.getValue(), nextKVUnit.getValue(),
                    "Expected next KV unit value to match sorted value");
            indexedClusterIterator.pollNextKVUnit();
        }
    }

    @Test
    void testPollNextKVUnit_returnsNextKVByRemovingFromQueue() throws IOException {
        for (KVUnit sortedKV : this.sortedKVs) {
            KVUnit nextKVUnit = indexedClusterIterator.pollNextKVUnit();
            assertArrayEquals(sortedKV.getKey(), nextKVUnit.getKey(),
                    "Expected next KV unit key to match sorted key");
            assertArrayEquals(sortedKV.getValue(), nextKVUnit.getValue(),
                    "Expected next KV unit value to match sorted value");
        }
    }

    @Test
    void testGetSSTInfo() {
        assertEquals(this.sstInfo, indexedClusterIterator.getSSTInfo());
        try {
            indexedClusterIterator.close();
        } catch (IllegalStateException _) {
            indexedClusterIterator = null;
        } catch (IOException _) {
        }
    }
}