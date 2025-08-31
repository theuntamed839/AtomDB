package io.github.theuntamed839.datastore4j.compaction;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.DbOptions;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.search.Search;
import io.github.theuntamed839.datastore4j.sstIO.SSTPersist;
import io.github.theuntamed839.datastore4j.table.SSTInfo;
import io.github.theuntamed839.datastore4j.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static io.github.theuntamed839.datastore4j.util.BytesConverter.bytes;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class CompactorTest {

    private Table table;
    private DbComponentProvider dbComponentProvider;
    private SSTPersist sstPersist;
    private Compactor compactor;
    private FileSystem jimfs;
    private Path dbPath;

    @BeforeEach
    public void setUp() throws IOException {
        DbOptions dbOptions = new DbOptions();
        dbOptions.disallowUseOfMMap();
        dbComponentProvider = new DbComponentProvider(dbOptions);
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        dbPath = Files.createTempDirectory(jimfs.getPath("/"), "CompactorTest_" + Instant.now().toEpochMilli());
        table = new Table(dbPath, new Search(dbComponentProvider), dbComponentProvider);
        compactor = new Compactor(table, mock(Search.class), dbPath, dbComponentProvider);
        sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (jimfs != null) {
            jimfs.close();
        }
        jimfs = null;
        compactor = null;
    }

    @Test
    void testTryCompaction_shouldCompactAllOverlappingSSTsInSameLevel() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator()); // overlapping with the next one
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-4}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator()); // overlapping with the previous one

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount = table.getSSTInfoSet(Level.LEVEL_ZERO).size();
        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        SortedSet<SSTInfo> ssts = table.getSSTInfoSet(Level.LEVEL_ZERO);
        assertFalse(ssts.stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_1.sst")),
                "Oldest SST should not be present after compaction");
        assertFalse(ssts.stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_2.sst")),
                "Oldest SST should not be present after compaction");
        assertEquals(sstCount - 2, ssts.size());
    }

    @Test
    void testTryCompaction_shouldPreferOldestOverlappingSSTsForCompaction() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator()); // overlapping with the next one
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-4}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator()); // overlapping with the previous one
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-6}, bytes("value1")),
                        new KVUnit(new byte[]{-5}, bytes("value2"))).iterator()); // overlapping with the next one
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-7}, bytes("value1")),
                        new KVUnit(new byte[]{-6}, bytes("value2"))).iterator()); // overlapping with the previous one

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount = table.getSSTInfoSet(Level.LEVEL_ZERO).size();
        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        SortedSet<SSTInfo> ssts = table.getSSTInfoSet(Level.LEVEL_ZERO);
        assertFalse(ssts.stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_1.sst")),
                "Oldest SST should not be present after compaction");
        assertFalse(ssts.stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_2.sst")),
                "Oldest SST should not be present after compaction");
        assertEquals(sstCount - 2, ssts.size());
    }

    @Test
    void testTryCompaction_shouldSkipNonOverlappingOldestAndCompactOverlappingSSTs() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator()); // non-overlapping
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-4}, bytes("value1")),
                        new KVUnit(new byte[]{-3}, bytes("value2"))).iterator()); // non-overlapping
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-6}, bytes("value1")),
                        new KVUnit(new byte[]{-5}, bytes("value2"))).iterator()); // overlapping with the next one
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-7}, bytes("value1")),
                        new KVUnit(new byte[]{-6}, bytes("value2"))).iterator()); // overlapping with the previous one

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount = table.getSSTInfoSet(Level.LEVEL_ZERO).size();
        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        SortedSet<SSTInfo> ssts = table.getSSTInfoSet(Level.LEVEL_ZERO);
        assertTrue(ssts.stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_1.sst")),
                "Oldest SST should be present after compaction");
        assertTrue(ssts.stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_2.sst")),
                "Oldest SST should be present after compaction");
        assertEquals(sstCount - 2, ssts.size());
    }

    @Test
    void testTryCompaction_shouldIncludeOverlappingNextLevelSSTsInCompaction() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-8}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-7}, bytes("value1")),
                        new KVUnit(new byte[]{-5}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-7}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount_LEVEL_ZERO = table.getSSTInfoSet(Level.LEVEL_ZERO).size();

        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        assertFalse(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_1.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_2.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_3.sst")));
        assertEquals(sstCount_LEVEL_ZERO - 2, table.getSSTInfoSet(Level.LEVEL_ZERO).size());
    }

    @Test
    void testTryCompaction_shouldPreferOldestSSTOverlappingSSTsWithNextLevel() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-6}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-4}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount_LEVEL_ZERO = table.getSSTInfoSet(Level.LEVEL_ZERO).size();

        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        assertFalse(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_1.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_2.sst")));
        assertEquals(sstCount_LEVEL_ZERO - 1, table.getSSTInfoSet(Level.LEVEL_ZERO).size());
    }

    @Test
    void testTryCompaction_onlyNewerOverlappingFilesIsTakenForCompactionFromNextLevel() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-7}, bytes("value1")),
                        new KVUnit(new byte[]{-3}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-8}, bytes("value1")),
                        new KVUnit(new byte[]{-2}, bytes("value2"))).iterator());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-6}, bytes("value1")),
                        new KVUnit(new byte[]{-4}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-7}, bytes("value1")),
                        new KVUnit(new byte[]{-5}, bytes("value2"))).iterator());

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount_LEVEL_ZERO = table.getSSTInfoSet(Level.LEVEL_ZERO).size();

        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        assertTrue(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_1.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_2.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_3.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_4.sst")));

        assertEquals(sstCount_LEVEL_ZERO - 2, table.getSSTInfoSet(Level.LEVEL_ZERO).size());
    }

    @Test
    void testTryCompaction_shouldAvoidNextLevelSSTsWithDependenciesToLimitCompactionSet() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-9}, bytes("value1")),
                        new KVUnit(new byte[]{-5}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-11}, bytes("value1")),
                        new KVUnit(new byte[]{-9}, bytes("value2"))).iterator());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-5}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-5}, bytes("value1")),
                        new KVUnit(new byte[]{-4}, bytes("value2"))).iterator());

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);
        var sstCount_LEVEL_ZERO = table.getSSTInfoSet(Level.LEVEL_ZERO).size();

        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close();

        assertTrue(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_1.sst")));
        assertTrue(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_2.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_3.sst")));
        assertFalse(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_4.sst")));


        assertEquals(sstCount_LEVEL_ZERO - 2, table.getSSTInfoSet(Level.LEVEL_ZERO).size());
    }

    @Test
    void testTryCompaction_shouldSkipCompactionWhenNoSSTsOverlap() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ONE, 2,
                List.of(new KVUnit(new byte[]{-4}, bytes("value1")),
                        new KVUnit(new byte[]{-3}, bytes("value2"))).iterator());

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-6}, bytes("value1")),
                        new KVUnit(new byte[]{-5}, bytes("value2"))).iterator());
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-8}, bytes("value1")),
                        new KVUnit(new byte[]{-7}, bytes("value2"))).iterator());

        generateSSTToTriggerCompactionNew(sstPersist, Level.LEVEL_ZERO);

        compactor.tryCompaction(Level.LEVEL_ZERO);
        compactor.close(); // wait for compaction to complete.

        assertTrue(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_1.sst")));
        assertTrue(table.getSSTInfoSet(Level.LEVEL_ONE).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_1_2.sst")));
        assertTrue(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_3.sst")));
        assertTrue(table.getSSTInfoSet(Level.LEVEL_ZERO).stream().anyMatch(sstInfo -> sstInfo.getSstPath().getFileName().toString().equals("SST_0_4.sst")));
    }

    private void generateSSTToTriggerCompactionNew(SSTPersist sstPersist, Level level) {
        while(table.getNumberOfFilesInLevel(level) < level.getMaxNumberOfFilesSupported() + 1) {
            var units = getUniqueEntries(100);
            try {
                sstPersist.writeSingleFile(level, units.size(), units.iterator());
            } catch (Exception e) {
                throw new RuntimeException("Failed to write SST", e);
            }
        }
    }

    private static List<KVUnit> getUniqueEntries(int entries) {
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);
        Random rand = new Random();

        while (map.size() < entries) {
            byte[] key = new byte[50];
            rand.nextBytes(key);

            if (key[0] < 50 || map.containsKey(key)) {
                continue;
            }

            byte[] value = new byte[500];
            rand.nextBytes(value);
            map.put(key, value);
        }

        List<KVUnit> units = new ArrayList<>(map.size());
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            units.add(new KVUnit(entry.getKey(), entry.getValue()));
        }
        return units;
    }
}