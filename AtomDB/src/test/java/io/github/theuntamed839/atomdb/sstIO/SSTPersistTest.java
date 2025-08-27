package io.github.theuntamed839.atomdb.sstIO;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.primitives.Longs;
import io.github.theuntamed839.atomdb.compaction.MergedClusterIterator;
import io.github.theuntamed839.atomdb.compaction.Pointer;
import io.github.theuntamed839.atomdb.compression.CompressionStrategyFactory;
import io.github.theuntamed839.atomdb.compression.DataCompressionStrategy;
import io.github.theuntamed839.atomdb.constants.DBConstant;
import io.github.theuntamed839.atomdb.db.DbOptions;
import io.github.theuntamed839.atomdb.db.KVUnit;
import io.github.theuntamed839.atomdb.level.Level;
import io.github.theuntamed839.atomdb.search.Checksums;
import io.github.theuntamed839.atomdb.search.Search;
import io.github.theuntamed839.atomdb.table.SSTInfo;
import io.github.theuntamed839.atomdb.util.BytesConverter;
import org.junit.jupiter.api.Test;

import static io.github.theuntamed839.atomdb.compaction.IndexedCluster.DUMMY_LOCATION;
import static org.junit.jupiter.api.Assertions.*;

import io.github.theuntamed839.atomdb.db.DbComponentProvider;
import io.github.theuntamed839.atomdb.table.Table;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

class SSTPersistTest {

    private Table table;
    private DbComponentProvider dbComponentProvider;
    private SSTPersist sstPersist;
    private Path dbPath;
    private FileSystem jimfs;
    private Search search;

    @BeforeEach
    void setUp() throws IOException {
        DbOptions dbOptions = new DbOptions();
        dbOptions.disallowUseOfMMap();
        dbComponentProvider = new DbComponentProvider(dbOptions);
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        dbPath = Files.createTempDirectory(jimfs.getPath("/"), "SSTPersistTest_" + UUID.randomUUID());
        search = new Search(dbComponentProvider);
        table = new Table(dbPath, search, dbComponentProvider);
        sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
    }

    @AfterEach
    void tearDown() throws IOException {
        try(var stream = Files.walk(dbPath)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to delete file: " + path, e);
                        }
                    });
        }
        if (jimfs != null) {
            jimfs.close();
        }
        jimfs = null;
    }

    @Test
    void writesSSTCorrectly() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, "value1".getBytes()),
                        new KVUnit(new byte[]{-1}, "value2".getBytes())).iterator());

        SortedSet<SSTInfo> sstInfos = table.getSSTInfoSet(Level.LEVEL_ZERO);

        SSTInfo sstInfo = SSTFileHelper.getSSTInfo(sstInfos.getFirst().getFileNameMeta(), dbComponentProvider);
        assertNotNull(sstInfo, "SSTInfo should not be null");
        assertEquals(2, sstInfo.getNumberOfEntries(), "SST should have 2 entries");
        assertTrue(sstInfo.mightContainElement(new byte[]{-2}), "Bloom filter should contain key -2");
        assertTrue(sstInfo.mightContainElement(new byte[]{-1}), "Bloom filter should contain key -1");
        assertFalse(sstInfo.mightContainElement(new byte[]{0}), "Bloom filter should not contain key 0");


        assertEquals(Level.LEVEL_ZERO, sstInfo.getLevel(), "SST should be at LEVEL_ZERO");
        assertEquals(DBConstant.CHECKSUM_TYPE.CRC32C, sstInfo.getChecksumType(), "Checksum type should be CRC32C");
        assertEquals(DBConstant.COMPRESSION_TYPE.LZ4, sstInfo.getCompressionStrategy(), "Compression type should be NONE");
        assertEquals(DBConstant.CLUSTER_SIZE, sstInfo.getSingleClusterSize(), "Single cluster size should match DB constant");
        assertTrue(sstInfo.isShortestCommonPrefixUsed(), "Shortest common prefix should be used");
        assertEquals(DBConstant.SST_VERSION, sstInfo.sstVersion(), "SST version should match DB constant");

        assertEquals(sstInfos.getFirst().getSstPath(), sstInfo.getSstPath(), "SST path should match");
        assertEquals(sstInfos.getFirst().getFileNameMeta(), sstInfo.getFileNameMeta(), "File name meta should match");
        assertEquals(sstInfos.getFirst().hashCode(), sstInfo.hashCode(), "SSTInfo hash code should match");
        assertEquals(sstInfos.getFirst().getSstPath(), sstInfo.getSstPath(), "SST normalized path should match");
        assertEquals(sstInfos.getFirst(), sstInfo, "Pointers should match");
        byte[] smallest = sstInfo.getSstKeyRange().getSmallest();
        assertArrayEquals(new byte[]{-2}, smallest, "Smallest key should be -2");
        byte[] largest = sstInfo.getSstKeyRange().getGreatest();
        assertArrayEquals(new byte[]{-1}, largest, "Largest key should be -1");
        assertEquals(2, sstInfo.getPointers().size(), "Pointer list should have 2 entries");
        assertArrayEquals(new byte[]{-2}, sstInfo.getPointers().get(0).key(), "First pointer should have 2 entries");

        //  sstInfo.getPointers().getLast().position() should be negative number
        assertTrue(sstInfo.getPointers().getLast().position() < 0, "Last pointer position should be negative");
        assertArrayEquals(sstInfo.getPointers().getLast().key(), new byte[]{-1}, "Last pointer key should be -1");

        // assert the last byte of file should be DBConstant.MARK_FILE_END
        byte[] lastBytes = Files.readAllBytes(sstInfo.getSstPath());
        assertArrayEquals(Arrays.copyOfRange(lastBytes, lastBytes.length - Longs.BYTES, lastBytes.length), BytesConverter.bytes(DBConstant.MARK_FILE_END), "Last bytes should match MARK_FILE_END");
    }

    @Test
    void writesSSTCorrectlyWithOnly1Data() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, "value1".getBytes())).iterator());

        SortedSet<SSTInfo> sstInfos = table.getSSTInfoSet(Level.LEVEL_ZERO);

        SSTInfo sstInfo = SSTFileHelper.getSSTInfo(sstInfos.getFirst().getFileNameMeta(), dbComponentProvider);
        assertNotNull(sstInfo, "SSTInfo should not be null");
        assertEquals(1, sstInfo.getNumberOfEntries(), "SST should have 2 entries");
        assertTrue(sstInfo.mightContainElement(new byte[]{-2}), "Bloom filter should contain key -2");
        assertFalse(sstInfo.mightContainElement(new byte[]{-1}), "Bloom filter should contain key -1");
        assertFalse(sstInfo.mightContainElement(new byte[]{-3}), "Bloom filter should not contain key 0");


        assertEquals(Level.LEVEL_ZERO, sstInfo.getLevel(), "SST should be at LEVEL_ZERO");
        assertEquals(DBConstant.CHECKSUM_TYPE.CRC32C, sstInfo.getChecksumType(), "Checksum type should be CRC32C");
        assertEquals(DBConstant.COMPRESSION_TYPE.LZ4, sstInfo.getCompressionStrategy(), "Compression type should be NONE");
        assertEquals(DBConstant.CLUSTER_SIZE, sstInfo.getSingleClusterSize(), "Single cluster size should match DB constant");
        assertTrue(sstInfo.isShortestCommonPrefixUsed(), "Shortest common prefix should be used");
        assertEquals(DBConstant.SST_VERSION, sstInfo.sstVersion(), "SST version should match DB constant");

        assertEquals(sstInfos.getFirst().getSstPath(), sstInfo.getSstPath(), "SST path should match");
        assertEquals(sstInfos.getFirst().getFileNameMeta(), sstInfo.getFileNameMeta(), "File name meta should match");
        assertEquals(sstInfos.getFirst().hashCode(), sstInfo.hashCode(), "SSTInfo hash code should match");
        assertEquals(sstInfos.getFirst().getSstPath(), sstInfo.getSstPath(), "SST normalized path should match");
        assertEquals(sstInfos.getFirst(), sstInfo, "Pointers should match");
        byte[] smallest = sstInfo.getSstKeyRange().getSmallest();
        assertArrayEquals(new byte[]{-2}, smallest, "Smallest key should be -2");
        byte[] largest = sstInfo.getSstKeyRange().getGreatest();
        assertArrayEquals(new byte[]{-2}, largest, "Largest key should be -1");
        assertEquals(2, sstInfo.getPointers().size(), "Pointer list should have 2 entries");
        assertArrayEquals(new byte[]{-2}, sstInfo.getPointers().get(0).key(), "First pointer should have 2 entries");

        //  sstInfo.getPointers().getLast().position() should be negative number
        assertTrue(sstInfo.getPointers().getLast().position() < 0, "Last pointer position should be negative");
        assertArrayEquals(sstInfo.getPointers().getLast().key(), new byte[]{-2}, "Last pointer key should be -1");

        // assert the last byte of file should be DBConstant.MARK_FILE_END
        byte[] lastBytes = Files.readAllBytes(sstInfo.getSstPath());
        assertArrayEquals(Arrays.copyOfRange(lastBytes, lastBytes.length - Longs.BYTES, lastBytes.length), BytesConverter.bytes(DBConstant.MARK_FILE_END), "Last bytes should match MARK_FILE_END");
    }


    @Test
    void writesSSTCorrectlyWithFullData() throws Exception {
        int totalEntries = 995;
        var kvs = new ArrayList<KVUnit>();
        for (int i = 0; i < totalEntries; i++) {
            kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
        }
        kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size(), kvs.iterator());
        SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

        testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
    }

    @Test
    void clusterTestFrom1to20() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);
            dbComponentProvider = new DbComponentProvider(dbOptions);
            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }

            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size(), kvs.iterator());
            SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

            testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
        }
    }

    @Test
    void pointerWorkCorrectlyWhenSizeIs_1() throws Exception {
        var kvs =  List.of(new KVUnit(("key" + 0).getBytes(), "value1".getBytes()));
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 1,
               kvs.iterator());
        SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

        testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
    }

    @Test
    void pointerWorkCorrectlyWhenSizeIs_12() throws Exception {
        var kvs = new ArrayList<KVUnit>();
        for (int i = 0; i <= 12; i++) {
            kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
        }

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 12,
                kvs.iterator());
        SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

        testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
    }

    @Test
    void handleAllDeleted() throws Exception {
        var kvs = new ArrayList<KVUnit>();
        for (int i = 0; i <= 999; i++) {
            kvs.add(new KVUnit(("key" + i).getBytes()));
        }
        kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 999,
                kvs.iterator());
        SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

        testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
    }

    @Test
    void handleSomeDeletes() throws Exception {
        var kvs = new ArrayList<KVUnit>();
        Random random = new Random();
        for (int i = 0; i <= 999; i++) {
            if(random.nextBoolean()) {
                kvs.add(new KVUnit(("key" + i).getBytes()));
            } else {
                kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }
        }
        kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

        sstPersist.writeSingleFile(Level.LEVEL_ONE, 999,
                kvs.iterator());
        SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ONE).getFirst();
        SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

        testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
    }

    @Test
    void changeInComparator() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);

            dbOptions.setComparator((left, right) -> {
                int minLength = Math.min(left.length, right.length);
                for (int i = 0; i < minLength; i++) {
                    int result = left[i] - right[i];
                    if (result != 0) {
                        return result;
                    }
                }
                return left.length - right.length;
            });
            dbComponentProvider = new DbComponentProvider(dbOptions);
            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }
            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size(), kvs.iterator());
            SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

            testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
        }
    }

    @Test
    void changeInCompressionSnappy() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);
            dbOptions.setCompressionType(DBConstant.COMPRESSION_TYPE.SNAPPY);
            dbComponentProvider = new DbComponentProvider(dbOptions);
            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }

            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size(), kvs.iterator());
            SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

            testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
        }
    }

    @Test
    void changeInCompressionNone() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);

            dbOptions.setCompressionType(DBConstant.COMPRESSION_TYPE.NONE);
            dbComponentProvider = new DbComponentProvider(dbOptions);

            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }
            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));
            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size(), kvs.iterator());
            SSTInfo alreadyStoredSSTInfo = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(alreadyStoredSSTInfo.getFileNameMeta(), dbComponentProvider);

            testFullSST(retrievedSSTInfo, alreadyStoredSSTInfo, kvs, dbComponentProvider);
        }
    }

    @Test
    void multipleFiles() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);
            dbComponentProvider = new DbComponentProvider(dbOptions);
            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }

            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size()/5, kvs.subList(0, kvs.size()/5).iterator());
            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size()/5, kvs.subList(kvs.size()/5, kvs.size()).iterator());

            var sstList  = table.getSSTInfoSet(Level.LEVEL_ZERO);
            MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(sstList, search, dbComponentProvider);
            sstPersist.writeManyFiles(Level.LEVEL_ONE, mergedClusterIterator, kvs.size()/2, sstList);

            var storedSst  = table.getSSTInfoSet(Level.LEVEL_ONE).getFirst();
            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(storedSst.getFileNameMeta(), dbComponentProvider);
            testFullSST(retrievedSSTInfo, storedSst, kvs, dbComponentProvider);
        }
    }

    @Test
    void multipleFileshandleSomeDeletes() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);
            dbComponentProvider = new DbComponentProvider(dbOptions);
            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            var random = new Random();
            for (int i = 1; i < totalEntries; i++) {
                if(random.nextBoolean()) {
                    kvs.add(new KVUnit(("key" + i).getBytes()));
                } else {
                    kvs.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
                }
            }

            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size()/5, kvs.subList(0, kvs.size()/5).iterator());
            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size()/5, kvs.subList(kvs.size()/5, kvs.size()).iterator());

            var sstList  = table.getSSTInfoSet(Level.LEVEL_ZERO);
            MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(sstList, search, dbComponentProvider);
            sstPersist.writeManyFiles(Level.LEVEL_ONE, mergedClusterIterator, kvs.size()/2, sstList);

            var storedSst  = table.getSSTInfoSet(Level.LEVEL_ONE).getFirst();
            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(storedSst.getFileNameMeta(), dbComponentProvider);
            testFullSSTAfterCOmpaction(retrievedSSTInfo, storedSst, kvs, dbComponentProvider);
        }
    }

    @Test
    void multipleFileshandleAllDeletes() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);
            dbComponentProvider = new DbComponentProvider(dbOptions);
            search = new Search(dbComponentProvider);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 997;
            var kvs = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                kvs.add(new KVUnit(("key" + i).getBytes()));
            }

            kvs.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));

            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size()/5, kvs.subList(0, kvs.size()/5).iterator());
            sstPersist.writeSingleFile(Level.LEVEL_ZERO, kvs.size()/5, kvs.subList(kvs.size()/5, kvs.size()).iterator());

            var sstList  = table.getSSTInfoSet(Level.LEVEL_ZERO);
            MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(sstList, search, dbComponentProvider);
            sstPersist.writeManyFiles(Level.LEVEL_ONE, mergedClusterIterator, kvs.size()/2, sstList);

            assertEquals(table.getSSTInfoSet(Level.LEVEL_ONE).size(), 0);
            assertEquals(table.getSSTInfoSet(Level.LEVEL_ZERO).size(), 0);
        }
    }

    @Test
    void multipleFileshandleUpdates() throws Exception {
        for (int clusterSize = 2; clusterSize <= 20 ; clusterSize++) {
            deleteRecursively(dbPath);
            DbOptions dbOptions = new DbOptions();
            dbOptions.disallowUseOfMMap();
            dbOptions.setClusterSize((byte) clusterSize);
            dbComponentProvider = new DbComponentProvider(dbOptions);
            table = new Table(dbPath, search, dbComponentProvider);
            sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);

            int totalEntries = 500;
            var first = new ArrayList<KVUnit>();
            for (int i = 1; i < totalEntries; i++) {
                first.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
            }

            first.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));
            sstPersist.writeSingleFile(Level.LEVEL_ZERO, first.size(), first.iterator());

            var set = new TreeSet<KVUnit>();
            var rand = new Random();
            for (int i = 500; i < 1000; i++) {
                if (rand.nextBoolean()) {
                    var k = rand.nextInt(500);
                    set.add(new KVUnit(("key" + k).getBytes(), ("value" + i).getBytes()));
                } else {
                    set.add(new KVUnit(("key" + i).getBytes(), ("value" + i).getBytes()));
                }
            }
            var second = new ArrayList<>(set);
            second.sort((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()));
            sstPersist.writeSingleFile(Level.LEVEL_ZERO, second.size(), second.iterator());

            var sstList  = table.getSSTInfoSet(Level.LEVEL_ZERO);
            MergedClusterIterator mergedClusterIterator = new MergedClusterIterator(sstList, search, dbComponentProvider);


            for (KVUnit kvUnit : first) {
                if (!set.contains(kvUnit)) {
                    set.add(kvUnit);
                }
            }
            var kvUnits = new ArrayList<>(set);
            sstPersist.writeManyFiles(Level.LEVEL_ONE, mergedClusterIterator, first.size()/2, sstList);

            assertEquals(table.getSSTInfoSet(Level.LEVEL_ONE).size(), 1);
            assertEquals(table.getSSTInfoSet(Level.LEVEL_ZERO).size(), 0);

            SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(table.getSSTInfoSet(Level.LEVEL_ONE).getFirst().getFileNameMeta(), dbComponentProvider);

            testFullSSTAfterCOmpaction(
                    retrievedSSTInfo,
                    table.getSSTInfoSet(Level.LEVEL_ONE).getFirst(),
                    new ArrayList<>(kvUnits), dbComponentProvider);
        }
    }

    private void testFullSSTAfterCOmpaction(SSTInfo retrievedSSTInfo, SSTInfo alreadyStoredSSTInfo, List<KVUnit> kvs, DbComponentProvider dbComponentProvider) throws IOException {
        kvs = kvs.stream().filter(kv -> !kv.isTombStone()).sorted((a, b) -> dbComponentProvider.getComparator().compare(a.getKey(), b.getKey()) ).collect(Collectors.toList());
        var totalEntries = kvs.size();

        assertNotNull(retrievedSSTInfo, "SSTInfo should not be null");
        assertEquals(totalEntries, retrievedSSTInfo.getNumberOfEntries(), "SST should have 2 entries");
//        assertTrue(retrievedSSTInfo.mightContainElement(("key" + (totalEntries - 1)).getBytes()), "Bloom filter should contain key -2");
//        assertFalse(retrievedSSTInfo.mightContainElement(("key" + (totalEntries + 1)).getBytes()), "Bloom filter should contain key -1");


        assertEquals(alreadyStoredSSTInfo.getLevel(), retrievedSSTInfo.getLevel(), "SST should be at LEVEL_ZERO");
        assertEquals(dbComponentProvider.getChecksumType(), retrievedSSTInfo.getChecksumType(), "Checksum type should be CRC32C");
        assertEquals(dbComponentProvider.getCompressionType(), retrievedSSTInfo.getCompressionStrategy(), "Compression type should be NONE");
        assertEquals(dbComponentProvider.getClusterSize(), retrievedSSTInfo.getSingleClusterSize(), "Single cluster size should match DB constant");
        assertTrue(retrievedSSTInfo.isShortestCommonPrefixUsed(), "Shortest common prefix should be used");
        assertEquals(DBConstant.SST_VERSION, retrievedSSTInfo.sstVersion(), "SST version should match DB constant");

        assertEquals(alreadyStoredSSTInfo.getSstPath(), retrievedSSTInfo.getSstPath(), "SST path should match");
        assertEquals(alreadyStoredSSTInfo.getFileNameMeta(), retrievedSSTInfo.getFileNameMeta(), "File name meta should match");
        assertEquals(alreadyStoredSSTInfo.hashCode(), retrievedSSTInfo.hashCode(), "SSTInfo hash code should match");
        assertEquals(alreadyStoredSSTInfo.getSstPath(), retrievedSSTInfo.getSstPath(), "SST normalized path should match");
        assertEquals(alreadyStoredSSTInfo, retrievedSSTInfo, "Pointers should match");

        byte[] smallest = retrievedSSTInfo.getSstKeyRange().getSmallest();
        assertArrayEquals(kvs.getFirst().getKey(), smallest, "Smallest key should be -2");
        byte[] largest = retrievedSSTInfo.getSstKeyRange().getGreatest();
        assertArrayEquals(kvs.getLast().getKey(), largest, "Largest key should be -1");
        assertEquals(Math.ceil((totalEntries * 1.0) / dbComponentProvider.getClusterSize()) + 1, retrievedSSTInfo.getPointers().size(), "Pointer list should have 2 entries");
        assertArrayEquals(kvs.getFirst().getKey(), retrievedSSTInfo.getPointers().getFirst().key(), "First pointer should have 2 entries");

        //  sstInfo.getPointers().getLast().position() should be negative number
        assertTrue(retrievedSSTInfo.getPointers().getLast().position() < 0, "Last pointer position should be negative");
        assertArrayEquals(kvs.getLast().getKey(), retrievedSSTInfo.getPointers().getLast().key(), "Last pointer key should be -1");

        // assert the last byte of file should be DBConstant.MARK_FILE_END
        byte[] allBytes = Files.readAllBytes(retrievedSSTInfo.getSstPath());
        assertArrayEquals(Arrays.copyOfRange(allBytes, allBytes.length - Longs.BYTES, allBytes.length), BytesConverter.bytes(DBConstant.MARK_FILE_END), "Last bytes should match MARK_FILE_END");


        ByteBuffer reader = ByteBuffer.wrap(Files.readAllBytes(retrievedSSTInfo.getSstPath()));
        DataCompressionStrategy decompressor = CompressionStrategyFactory.getCompressionStrategy(retrievedSSTInfo.getCompressionStrategy());
        for (int i = 0, j = 0; i < totalEntries; j++) {
            byte[] key = kvs.get(i).getKey();
            var pointer = retrievedSSTInfo.getPointers().get(j);
            assertArrayEquals(key, pointer.key(), "Pointer key should match the key at index " + i);

            reader.position((int) pointer.position());
            Checksums checksums = getChecksums(retrievedSSTInfo.getSingleClusterSize(), reader);

            List<Integer> locations = getLocationList(reader, retrievedSSTInfo.getSingleClusterSize());
            int commonPrefix = reader.getInt();
            long beforeReadingKV = reader.position();
            int kvCount = getTotalKVsInCluster(locations);
            for (int k = 0; k < kvCount; k++, i++) {
                var byteArray = decompressBlock(locations, k, reader, decompressor);
                var block = ByteBuffer.wrap(byteArray);

                KVUnit kvUnit = parseKV(pointer, block, commonPrefix);

                assertArrayEquals(kvs.get(i).getKey(), kvUnit.getKey());
                if (!kvUnit.isTombStone()) {
                    assertArrayEquals(kvs.get(i).getValue(), kvUnit.getValue(), "expected " + new String(kvs.get(i).getValue()) + " but got " + new String(kvUnit.getValue()) + " at index " + i);
                } else {
                    assertNull(kvs.get(i).getValue(), "Value should be null for tombstone at index " + i);
                }

                assertEquals(checksums.checksums()[k], getKeyChecksum(kvUnit.getKey()), "Checksum should match for key at index " + i);
            }
            if (reader.position() != locations.get(kvCount) + beforeReadingKV) {
                throw new IllegalStateException("Reader position does not match expected end of cluster" + reader.position() + " != " + (locations.get(kvCount) + beforeReadingKV));
            }
        }
    }

    private static void testFullSST(SSTInfo retrievedSSTInfo, SSTInfo alreadyStoredSSTInfo, List<KVUnit> kvs, DbComponentProvider dbComponentProvider) throws IOException {
        var totalEntries = kvs.size();
        assertNotNull(retrievedSSTInfo, "SSTInfo should not be null");
        assertEquals(totalEntries, retrievedSSTInfo.getNumberOfEntries(), "SST should have 2 entries");
        assertTrue(retrievedSSTInfo.mightContainElement(("key" + (totalEntries - 1)).getBytes()), "Bloom filter should contain key -2");
        assertFalse(retrievedSSTInfo.mightContainElement(("key" + (totalEntries + 1)).getBytes()), "Bloom filter should contain key -1");


        assertEquals(alreadyStoredSSTInfo.getLevel(), retrievedSSTInfo.getLevel(), "SST should be at LEVEL_ZERO");
        assertEquals(dbComponentProvider.getChecksumType(), retrievedSSTInfo.getChecksumType(), "Checksum type should be CRC32C");
        assertEquals(dbComponentProvider.getCompressionType(), retrievedSSTInfo.getCompressionStrategy(), "Compression type should be NONE");
        assertEquals(dbComponentProvider.getClusterSize(), retrievedSSTInfo.getSingleClusterSize(), "Single cluster size should match DB constant");
        assertTrue(retrievedSSTInfo.isShortestCommonPrefixUsed(), "Shortest common prefix should be used");
        assertEquals(DBConstant.SST_VERSION, retrievedSSTInfo.sstVersion(), "SST version should match DB constant");

        assertEquals(alreadyStoredSSTInfo.getSstPath(), retrievedSSTInfo.getSstPath(), "SST path should match");
        assertEquals(alreadyStoredSSTInfo.getFileNameMeta(), retrievedSSTInfo.getFileNameMeta(), "File name meta should match");
        assertEquals(alreadyStoredSSTInfo.hashCode(), retrievedSSTInfo.hashCode(), "SSTInfo hash code should match");
        assertEquals(alreadyStoredSSTInfo.getSstPath(), retrievedSSTInfo.getSstPath(), "SST normalized path should match");
        assertEquals(alreadyStoredSSTInfo, retrievedSSTInfo, "Pointers should match");

        byte[] smallest = retrievedSSTInfo.getSstKeyRange().getSmallest();
        assertArrayEquals(kvs.getFirst().getKey(), smallest, "Smallest key should be -2");
        byte[] largest = retrievedSSTInfo.getSstKeyRange().getGreatest();
        assertArrayEquals(kvs.getLast().getKey(), largest, "Largest key should be -1");
        assertEquals(Math.ceil((totalEntries * 1.0) / dbComponentProvider.getClusterSize()) + 1, retrievedSSTInfo.getPointers().size(), "Pointer list should have 2 entries");
        assertArrayEquals(kvs.getFirst().getKey(), retrievedSSTInfo.getPointers().getFirst().key(), "First pointer should have 2 entries");

        //  sstInfo.getPointers().getLast().position() should be negative number
        assertTrue(retrievedSSTInfo.getPointers().getLast().position() < 0, "Last pointer position should be negative");
        assertArrayEquals(kvs.getLast().getKey(), retrievedSSTInfo.getPointers().getLast().key(), "Last pointer key should be -1");

        // assert the last byte of file should be DBConstant.MARK_FILE_END
        byte[] allBytes = Files.readAllBytes(retrievedSSTInfo.getSstPath());
        assertArrayEquals(Arrays.copyOfRange(allBytes, allBytes.length - Longs.BYTES, allBytes.length), BytesConverter.bytes(DBConstant.MARK_FILE_END), "Last bytes should match MARK_FILE_END");


        ByteBuffer reader = ByteBuffer.wrap(Files.readAllBytes(retrievedSSTInfo.getSstPath()));
        DataCompressionStrategy decompressor = CompressionStrategyFactory.getCompressionStrategy(retrievedSSTInfo.getCompressionStrategy());
        for (int i = 0, j = 0; i < totalEntries; j++) {
            byte[] key = kvs.get(i).getKey();
            var pointer = retrievedSSTInfo.getPointers().get(j);
            assertArrayEquals(key, pointer.key(), "Pointer key should match the key at index " + i);

            reader.position((int) pointer.position());
            Checksums checksums = getChecksums(retrievedSSTInfo.getSingleClusterSize(), reader);

            List<Integer> locations = getLocationList(reader, retrievedSSTInfo.getSingleClusterSize());
            int commonPrefix = reader.getInt();
            long beforeReadingKV = reader.position();
            int kvCount = getTotalKVsInCluster(locations);
            for (int k = 0; k < kvCount; k++, i++) {
                var byteArray = decompressBlock(locations, k, reader, decompressor);
                var block = ByteBuffer.wrap(byteArray);

                KVUnit kvUnit = parseKV(pointer, block, commonPrefix);

                assertArrayEquals(kvs.get(i).getKey(), kvUnit.getKey());
                if (!kvUnit.isTombStone()) {
                    assertArrayEquals(kvs.get(i).getValue(), kvUnit.getValue(), "Value should match for key at index " + i);
                } else {
                    assertNull(kvs.get(i).getValue(), "Value should be null for tombstone at index " + i);
                }

                assertEquals(checksums.checksums()[k], getKeyChecksum(kvUnit.getKey()), "Checksum should match for key at index " + i);
            }
            if (reader.position() != locations.get(kvCount) + beforeReadingKV) {
                throw new IllegalStateException("Reader position does not match expected end of cluster" + reader.position() + " != " + (locations.get(kvCount) + beforeReadingKV));
            }
        }
    }

    private static long getKeyChecksum(byte[] key) {
        Checksum checksum = new CRC32C();
        checksum.reset();
        checksum.update(key);
        return checksum.getValue();
    }

    private static Checksums getChecksums(int singleClusterSize, ByteBuffer reader) {
        var arr = new long[singleClusterSize];
        for (int i = 0; i < singleClusterSize; i++) {
            arr[i] = reader.getLong();
        }
        return new Checksums(arr);
    }

    private static List<Integer> getLocationList(ByteBuffer wrap, int sizeOfCluster) {
        List<Integer> locations = new ArrayList<>();
        for (int i = 0; i < sizeOfCluster + 1; i++) {
            locations.add(wrap.getInt());
        }
        return locations;
    }

    private static int getTotalKVsInCluster(List<Integer> locations) {
        for (int i = 0; i < locations.size(); i++) {
            if (locations.get(i) == DUMMY_LOCATION) {
                return i - 1;
            }
        }
        return locations.size() - 1;
    }

    private static byte[] decompressBlock(List<Integer> locations, int index, ByteBuffer bytes, DataCompressionStrategy decompressor) throws IOException {
        byte[] block = new byte[locations.get(index + 1) - locations.get(index)];
        bytes.get(block);
        return decompressor.decompress(block);
    }

    private static KVUnit parseKV(Pointer pointer, ByteBuffer block, int commonPrefix) {
        int keyLength = block.getInt();

        byte[] key = new byte[keyLength + commonPrefix];
        System.arraycopy(pointer.key(), 0, key, 0, commonPrefix);
        block.get(key, commonPrefix, keyLength);

        byte marker = block.get();

        if (KVUnit.isTombStone(marker)) {
            return new KVUnit(key);
        } else {
            int valueLength = block.getInt();
            byte[] value = new byte[valueLength];
            block.get(value);
            return new KVUnit(key, value);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (!dir.equals(root)) {  // don’t delete the root itself
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
