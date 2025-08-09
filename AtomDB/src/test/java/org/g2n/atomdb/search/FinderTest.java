package org.g2n.atomdb.search;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.checksum.Crc32cChecksum;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.DbOptions;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.sstIO.SSTPersist;
import org.g2n.atomdb.table.SSTInfo;
import org.g2n.atomdb.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FinderTest {
    private Table table;
    private DbComponentProvider dbComponentProvider;
    private SSTPersist sstPersist;
    private FileSystem jimfs;
    private Path dbPath;

    @BeforeEach
    public void setUp() throws IOException {
        DbOptions dbOptions = new DbOptions();
        dbOptions.disallowUseOfMMap();
        dbComponentProvider = new DbComponentProvider(dbOptions);
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        dbPath = Files.createTempDirectory(jimfs.getPath("/"), "FinderTest_" + Instant.now().toEpochMilli());
        table = new Table(dbPath, new Search(dbComponentProvider), dbComponentProvider);
        sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
    }

    @Test
    void testFind_shouldReturnTheStoredKV() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, "value1".getBytes()),
                        new KVUnit(new byte[]{-1}, "value2".getBytes())).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy());


        KVUnit kvUnit = finder.find(new byte[]{-2}, getKeyChecksum(new byte[]{-2}));
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-2}, kvUnit.getKey());
        assertArrayEquals("value1".getBytes(), kvUnit.getValue());
        kvUnit = finder.find(new byte[]{-1}, getKeyChecksum(new byte[]{-1}));
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-1}, kvUnit.getKey());
        assertArrayEquals("value2".getBytes(), kvUnit.getValue());
    }

    @Test
    void testFind_shouldReturnNullForNotFoundKey() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, "value1".getBytes()),
                        new KVUnit(new byte[]{-1}, "value2".getBytes())).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy());


        KVUnit kvUnit = finder.find(new byte[]{-3}, getKeyChecksum(new byte[]{-3}));
        assertNull(kvUnit);
    }


    @Test
    void testFind_shouldHandleMultipleEntriesWithSameChecksum() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 3,
                List.of(new KVUnit(new byte[]{-3}, "value3".getBytes()),
                        new KVUnit(new byte[]{-2}, "value2".getBytes()),
                        new KVUnit(new byte[]{-1}, "value1".getBytes())).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();

        var wrap = ByteBuffer.wrap(Files.readAllBytes(first.getSstPath()));
        wrap.position(18);
        wrap.putLong(getKeyChecksum(new byte[]{-2}));
        wrap.putLong(getKeyChecksum(new byte[]{-2}));
        wrap.flip();
        Files.write(first.getSstPath(), wrap.array());

        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy());

        long checksum = getKeyChecksum(new byte[]{-2});

        KVUnit kvUnit = finder.find(new byte[]{-2}, checksum);
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-2}, kvUnit.getKey());
        assertArrayEquals("value2".getBytes(), kvUnit.getValue());

        kvUnit = finder.find(new byte[]{-3}, checksum);
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-3}, kvUnit.getKey());
        assertArrayEquals("value3".getBytes(), kvUnit.getValue());

        kvUnit = finder.find(new byte[]{-1}, getKeyChecksum(new byte[]{-1}));
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-1}, kvUnit.getKey());
        assertArrayEquals("value1".getBytes(), kvUnit.getValue());
    }

    @Test
    void testFind_shouldReturnNullForNonExistentKeyWithSameChecksum() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, "value1".getBytes()),
                        new KVUnit(new byte[]{-1}, "value2".getBytes())).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy());

        // Key with same checksum but not present
        KVUnit kvUnit = finder.find(new byte[]{-3}, getKeyChecksum(new byte[]{-2}));
        assertNull(kvUnit);
    }

    private static long getKeyChecksum(byte[] key) {
        Crc32cChecksum crc32cChecksum = Crc32cChecksum.getInstance();//new Crc32cChecksum();
        return crc32cChecksum.compute(key);
    }
}