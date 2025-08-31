package io.github.theuntamed839.datastore4j.search;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.DbOptions;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.sstIO.SSTPersist;
import io.github.theuntamed839.datastore4j.table.SSTInfo;
import io.github.theuntamed839.datastore4j.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import static io.github.theuntamed839.datastore4j.util.BytesConverter.bytes;
import static org.junit.jupiter.api.Assertions.*;

class FinderTest {
    private static final ThreadLocal<Checksum> crc32cThreadLocal = ThreadLocal.withInitial(CRC32C::new);
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
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy(), Arrays::compare);


        KVUnit kvUnit = finder.find(new byte[]{-2}, getKeyChecksum(new byte[]{-2}));
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-2}, kvUnit.getKey());
        assertArrayEquals(bytes("value1"), kvUnit.getValue());
        kvUnit = finder.find(new byte[]{-1}, getKeyChecksum(new byte[]{-1}));
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-1}, kvUnit.getKey());
        assertArrayEquals(bytes("value2"), kvUnit.getValue());
    }

    @Test
    void testFind_shouldReturnNullForNotFoundKey() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy(), Arrays::compare);


        KVUnit kvUnit = finder.find(new byte[]{-3}, getKeyChecksum(new byte[]{-3}));
        assertNull(kvUnit);
    }


    @Test
    void testFind_shouldHandleMultipleEntriesWithSameChecksum() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 3,
                List.of(new KVUnit(new byte[]{-3}, bytes("value3")),
                        new KVUnit(new byte[]{-2}, bytes("value2")),
                        new KVUnit(new byte[]{-1}, bytes("value1"))).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();

        var wrap = ByteBuffer.wrap(Files.readAllBytes(first.getSstPath()));
        wrap.position(18);
        wrap.putLong(getKeyChecksum(new byte[]{-2}));
        wrap.putLong(getKeyChecksum(new byte[]{-2}));
        wrap.flip();
        Files.write(first.getSstPath(), wrap.array());

        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy(), Arrays::compare);

        long checksum = getKeyChecksum(new byte[]{-2});

        KVUnit kvUnit = finder.find(new byte[]{-2}, checksum);
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-2}, kvUnit.getKey());
        assertArrayEquals(bytes("value2"), kvUnit.getValue());

        kvUnit = finder.find(new byte[]{-3}, checksum);
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-3}, kvUnit.getKey());
        assertArrayEquals(bytes("value3"), kvUnit.getValue());

        kvUnit = finder.find(new byte[]{-1}, getKeyChecksum(new byte[]{-1}));
        assertNotNull(kvUnit);
        assertArrayEquals(new byte[]{-1}, kvUnit.getKey());
        assertArrayEquals(bytes("value1"), kvUnit.getValue());
    }

    @Test
    void testFind_shouldReturnNullForNonExistentKeyWithSameChecksum() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, bytes("value1")),
                        new KVUnit(new byte[]{-1}, bytes("value2"))).iterator());

        SSTInfo first = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();
        Finder finder = new Finder(first.getPointers(), dbComponentProvider.getIOReader(first.getSstPath()),
                first.getSingleClusterSize(), first.getCompressionStrategy(), Arrays::compare);

        // Key with same checksum but not present
        KVUnit kvUnit = finder.find(new byte[]{-3}, getKeyChecksum(new byte[]{-2}));
        assertNull(kvUnit);
    }

    private static long getKeyChecksum(byte[] key) {
        Checksum checksum = crc32cThreadLocal.get();
        checksum.reset();
        checksum.update(key);
        return checksum.getValue();
    }
}