package io.github.theuntamed839.datastore4j.compaction;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.theuntamed839.datastore4j.compression.CompressionStrategyFactory;
import io.github.theuntamed839.datastore4j.compression.DataCompressionStrategy;
import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.sstIO.IOFileChannelReader;
import io.github.theuntamed839.datastore4j.sstIO.SSTHeader;
import io.github.theuntamed839.datastore4j.db.ExpandingByteBuffer;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

class IndexedClusterTest{
    private IndexedCluster cluster;

    @BeforeEach
    void setUp() {
        var header = new SSTHeader(
                (byte) 123,
                Level.LEVEL_ZERO.value(),
                DBConstant.CHECKSUM_TYPE.CRC32C,
                DBConstant.COMPRESSION_TYPE.LZ4,
                (byte) 3,
                true,
                0,
                0,
                0
        );
        cluster = new IndexedCluster(header, Arrays::compare);
    }

    private IndexedCluster getClusterOfSize(int size) {
        var header = new SSTHeader(
                (byte) 123,
                Level.LEVEL_ZERO.value(),
                DBConstant.CHECKSUM_TYPE.CRC32C,
                DBConstant.COMPRESSION_TYPE.LZ4,
                (byte) size,
                true,
                0,
                0,
                0
        );
        return new IndexedCluster(header, Arrays::compare);
    }

    @Test
    void testAdd_Success() {
        KVUnit kv1 = new KVUnit(new byte[]{0x01}, new byte[]{0x01});
        KVUnit kv2 = new KVUnit(new byte[]{0x02}, new byte[]{0x02});

        cluster.add(kv1);
        cluster.add(kv2);

        assertEquals(2, cluster.getNumberOfEntries());
        assertEquals(kv1.getUnitSize() + kv2.getUnitSize(), cluster.getTotalSize());
    }

    @Test
    void testAdd_ThrowsException_WhenFull() {
        cluster.add(new KVUnit(new byte[]{0x01}, new byte[]{0x01}));
        cluster.add(new KVUnit(new byte[]{0x02}, new byte[]{0x02}));
        cluster.add(new KVUnit(new byte[]{0x03}, new byte[]{0x03}));

        assertThrows(
                IllegalStateException.class,
                () -> cluster.add(new KVUnit(new byte[]{0x04}, new byte[]{0x04}))
        );
    }

    @Test
    void testAdd_ThrowsException_WhenKeyNotIncreasing() {
        cluster.add(new KVUnit(new byte[]{0x02}, new byte[]{0x01}));

        assertThrows(
                IllegalArgumentException.class,
                () -> cluster.add(new KVUnit(new byte[]{0x01}, new byte[]{0x01}))
        );
    }

    @Test
    void testIsFull_ReturnsFalse_WhenFull() {
        KVUnit kv1 = new KVUnit(new byte[]{0x01}, new byte[]{0x01});
        KVUnit kv2 = new KVUnit(new byte[]{0x02}, new byte[]{0x02});
        KVUnit kv3 = new KVUnit(new byte[]{0x03}, new byte[]{0x03});

        cluster.add(kv1);
        cluster.add(kv2);
        cluster.add(kv3);
        assertTrue(cluster.isFull());
    }

    @Test
    void testGetFirstKey_ReturnsFirstKey_WhenAvailable() {
        KVUnit kv1 = new KVUnit(new byte[]{0x01}, new byte[]{0x01});
        KVUnit kv2 = new KVUnit(new byte[]{0x02}, new byte[]{0x02});
        KVUnit kv3 = new KVUnit(new byte[]{0x03}, new byte[]{0x03});

        cluster.add(kv1);
        cluster.add(kv2);
        cluster.add(kv3);
        byte[] firstKey = cluster.getFirstKey();
        assertArrayEquals(kv1.getKey(), firstKey);
    }

    @Test
    void testGetLastKey_ReturnsLastKey_WhenAvailable() {
        KVUnit kv1 = new KVUnit(new byte[]{0x01}, new byte[]{0x01});
        KVUnit kv2 = new KVUnit(new byte[]{0x02}, new byte[]{0x02});
        KVUnit kv3 = new KVUnit(new byte[]{0x03}, new byte[]{0x03});

        cluster.add(kv1);
        cluster.add(kv2);
        cluster.add(kv3);
        byte[] lastKey = cluster.getLastKey();
        assertArrayEquals(kv3.getKey(), lastKey);
    }

    @Test
    public void testStoreAsBytes_worksForAllNumberOfKeys() throws IOException {
        for (int i = 1; i <= 100; i++) {
            validateStoreBytes(i);
        }
    }

    @Test
    public void testFillQueue_worksForAllNumberOfKeys() throws IOException {
        for (int i = 1; i <= 100; i++) {
            validateFillQueue(i);
        }
    }

    void validateFillQueue(int numberOfKeys) throws IOException {
        IndexedCluster cluster = getClusterOfSize(numberOfKeys);
        List<KVUnit> units = loadAndGet(numberOfKeys, cluster);
        Pointer pointer = new Pointer(cluster.getFirstKey(), 0, Arrays::compare);
        var queue = new ArrayDeque<KVUnit>();
        var writer = new ExpandingByteBuffer();
        cluster.storeAsBytes(writer);

        var jimfs = Jimfs.newFileSystem(Configuration.unix());
        Path filePath = Files.createFile(jimfs.getPath("test.sst"));
        writeToFile(writer, filePath);

        try (IOFileChannelReader reader = new IOFileChannelReader(filePath)) {
            IndexedCluster.fillQueue(reader, pointer, (byte) numberOfKeys, DBConstant.COMPRESSION_TYPE.LZ4, queue);
            assertEquals(numberOfKeys, queue.size());
            for (KVUnit unit : units) {
                KVUnit dequeuedUnit = queue.poll();
                assertNotNull(dequeuedUnit);
                assertArrayEquals(unit.getKey(), dequeuedUnit.getKey());
                if (!unit.isTombStone()) {
                    assertArrayEquals(unit.getValue(), dequeuedUnit.getValue());
                }
            }
            assertTrue(queue.isEmpty());
        } finally {
            jimfs.close();
        }
    }

    private static void writeToFile(ExpandingByteBuffer writer, Path filePath) throws IOException {
        byte[] array = new byte[writer.position()];
        writer.flip();
        writer.get(array);
        Files.write(filePath, array);
    }

    private long getChecksum(byte[] key) {
        var checksum = new CRC32C();
        checksum.reset();
        checksum.update(key);
        return checksum.getValue();
    }

    void validateStoreBytes(int numberOfKeys) throws IOException {
        var buffer = new ExpandingByteBuffer();
        IndexedCluster cluster = getClusterOfSize(numberOfKeys);

        List<KVUnit> units = loadAndGet(numberOfKeys, cluster);

        cluster.storeAsBytes(buffer);
        byte[] array = new byte[buffer.position()];
        buffer.flip();
        buffer.get(array);
        var reader = ByteBuffer.wrap(array);
        assertChecksum(units, reader);

        List<Integer> locations = retrieveLocations(numberOfKeys, reader);
        int commonPrefix = reader.getInt();

        long startOfKVBlock = reader.position();
        for (int i = 0; i < numberOfKeys; i++) {
            reader.position((int) (startOfKVBlock + locations.get(i)));
            var block = new byte[locations.get(i + 1) - locations.get(i)];
            reader.get(block);
            DataCompressionStrategy compressionStrategy = CompressionStrategyFactory.getCompressionStrategy(DBConstant.COMPRESSION_TYPE.LZ4);
            block = compressionStrategy.decompress(block);
            ByteBuffer wrap = ByteBuffer.wrap(block);
            assertKVBlock(units.get(i), commonPrefix, wrap);
        }
        assertEquals(0, reader.remaining()); // Ensure we read everything
    }

    private void assertKVBlock(KVUnit kvUnit, int commonPrefix, ByteBuffer wrap) {
        int keySize = wrap.getInt();
        byte[] key = new byte[keySize + commonPrefix];
        System.arraycopy(kvUnit.getKey(), 0, key, 0, commonPrefix);
        wrap.get(key, commonPrefix, keySize);
        assertArrayEquals(kvUnit.getKey(), key);

        byte marker = wrap.get();
        assertEquals(kvUnit.getTombStoneValue(), marker);

        if (kvUnit.isTombStone()) {
            return; // No value for deleted keys
        }

        int valueSize = wrap.getInt();
        byte[] value = new byte[valueSize];
        wrap.get(value);
        assertArrayEquals(kvUnit.getValue(), value);
    }

    private List<Integer> retrieveLocations(int numberOfKeysInCluster, ByteBuffer reader) {
        var list = new ArrayList<Integer>();
        for (int i = 0; i < numberOfKeysInCluster + 1; i++) {
            list.add(reader.getInt());
        }
        return list;
    }

    private void assertChecksum(List<KVUnit> units, ByteBuffer reader) {
        for (KVUnit unit : units) {
            assertEquals(getChecksum(unit.getKey()), reader.getLong());
        }
    }

    private List<KVUnit> loadAndGet(int numberOfKeys, IndexedCluster cluster) {
        var rand = new Random();
        var kvs = new ArrayList<KVUnit>();
        var keySet = new TreeSet<byte[]>(Arrays::compare);
        while (kvs.size() < numberOfKeys) {
            byte[] key = new byte[rand.nextInt(1, 100)];
            rand.nextBytes(key);

            if (keySet.contains(key)) {
                continue; // Skip if key already exists
            }else{
                keySet.add(key); // Add to set to ensure uniqueness
            }

            byte[] value = new byte[rand.nextInt(1, 100)];
            rand.nextBytes(value);
            KVUnit kvUnit = new KVUnit(key, value);
            kvs.add(kvUnit);
        }
        kvs.sort((a, b) -> Arrays.compare(a.getKey(), b.getKey()));
        kvs.forEach(cluster::add);
        return kvs;
    }
}