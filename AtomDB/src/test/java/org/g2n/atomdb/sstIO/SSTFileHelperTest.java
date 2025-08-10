package org.g2n.atomdb.sstIO;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.DbOptions;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.search.Search;
import org.g2n.atomdb.table.SSTInfo;
import org.g2n.atomdb.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SSTFileHelperTest {
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
        dbPath = Files.createTempDirectory(jimfs.getPath("/"), "SSTFileHelperTest_" + Instant.now().toEpochMilli());
        table = new Table(dbPath, new Search(dbComponentProvider), dbComponentProvider);
        sstPersist = new SSTPersist(table, dbPath, dbComponentProvider);
    }

    @Test
    void testGetSSTInfo() throws Exception {
        sstPersist.writeSingleFile(Level.LEVEL_ZERO, 2,
                List.of(new KVUnit(new byte[]{-2}, "value1".getBytes()),
                        new KVUnit(new byte[]{-1}, "value2".getBytes())).iterator());

        SSTInfo sst = table.getSSTInfoSet(Level.LEVEL_ZERO).getFirst();

        SSTInfo retrievedSST = SSTFileHelper.getSSTInfo(sst.getFileNameMeta(), dbComponentProvider);

        assertNotNull(retrievedSST);
        assertEquals(sst.getLevel(), retrievedSST.getLevel());
        assertEquals(sst.getSstPath(), retrievedSST.getSstPath());
        assertEquals(sst.getNumberOfEntries(), retrievedSST.getNumberOfEntries());
        assertEquals(sst.getSingleClusterSize(), retrievedSST.getSingleClusterSize());
        assertEquals(sst.getCompressionStrategy(), retrievedSST.getCompressionStrategy());
        assertEquals(sst.getPointers().size(), retrievedSST.getPointers().size());
        for (int i = 0; i < sst.getPointers().size(); i++) {
            assertArrayEquals(sst.getPointers().get(i).key(), retrievedSST.getPointers().get(i).key());
            assertEquals(sst.getPointers().get(i).position(), retrievedSST.getPointers().get(i).position());
        }
    }
}