import Checksum.*;
import Compaction.SSTPersist;
import Constants.DBConstant;
import Level.Level;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import db.DBComparator;
import db.KVUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sstIo.SSTKeyRange;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class SSTPersistCorrectnessTest {
    private File testFile;
    private FileSystem fs;

    @BeforeEach
    void setUp() throws IOException {
        this.fs = Jimfs.newFileSystem(Configuration.unix());
        Path pathToStore = fs.getPath("");
        Path filePath = pathToStore.resolve("testFile.sst");
        Files.createFile(filePath);
        this.testFile = new File("testFile.sst");
    }

    @AfterEach
    void tearDown() throws IOException {
        fs.close();
    }

    @Test
    void headerRetrievalTest() {
        //stage
        int numberOfEntries = 50000;
        List<KVUnit> kvs = new ArrayList<>(50000);
        Random random = new Random();
        for (int i = 0; i < numberOfEntries; i++) {
            byte[] key = new byte[500];
            byte[] value = new byte[500];
            random.nextBytes(key);
            random.nextBytes(value);
            kvs.add(new KVUnit(key, value));
        }

        kvs.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getKey(), b.getKey()));

        // suuuuutt
        new SSTPersist(testFile, kvs.iterator(),
                new SSTKeyRange(kvs.getFirst().getKey(), kvs.getLast().getKey()),
                numberOfEntries, 10);

        // verification
        try (
                RandomAccessFile randomAccessFile = new RandomAccessFile(testFile, "r");
                FileChannel channel = randomAccessFile.getChannel();
        ) {
            var map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

            int headerSize = map.getInt();
            map.limit(headerSize);
            Assertions.assertEquals(DBConstant.SST_VERSION, map.get());
            Assertions.assertEquals(Level.LEVEL_ZERO.value(), map.get());
            Assertions.assertEquals(DBConstant.CRC32C_CHECKSUM_TYPE, map.get());
            Assertions.assertEquals(DBConstant.LZ4_COMPRESSION_TYPE, map.get());
            Assertions.assertEquals(DBConstant.CLUSTER_SIZE, map.get());
            Assertions.assertEquals(DBConstant.SHORTEST_COMMON_PREFIX_USED, map.get());
            Assertions.assertEquals(numberOfEntries, map.getLong());
            long filterPosition = map.getLong();
            long pointersPosition = map.getLong();


            Assertions.assertEquals(kvs.getFirst().getKey().length, map.getInt());
            byte[] firstKey = new byte[kvs.getFirst().getKey().length];
            map.get(firstKey);
            Assertions.assertEquals(kvs.getFirst().getKey(), firstKey);
            Assertions.assertEquals(kvs.getLast().getKey().length, map.getInt());
            byte[] lastKey = new byte[kvs.getLast().getKey().length];
            map.get(lastKey);
            Assertions.assertEquals(kvs.getLast().getKey(), lastKey);

            Checksum checksumProvide = new Crc32cChecksum();
            long checksum = checksumProvide.compute(firstKey, lastKey);
            Assertions.assertEquals(checksum, map.getLong());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
