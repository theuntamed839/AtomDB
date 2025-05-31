
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static org.g2n.atomdb.util.BytesConverter.bytes;

public class WalTest {
    public static final String VALUE = "value".repeat(50);
    DbOptions opt ;
    DBImpl db;
    private Path dbDirectoryPath;

    @BeforeEach
    public void setup() throws Exception {
        opt = new DbOptions();
        opt.setSSTFileSize(10 * 1024 * 1024); //should not trigger sst file creation todo we could use the jmfs to check if any ssts were created and invalidate the test
        dbDirectoryPath = Path.of(this.getClass().getName() + "_" + Instant.now().getEpochSecond() + "_DB");
//        FileSystem fs = Jimfs.newFileSystem(Configuration.windows());
//        Path foo = fs.getPath("JIMFS_DIR");
//        dbDirectoryPath = foo;
//        opt.disallowUseOfMMap();
        db = new DBImpl(dbDirectoryPath, opt);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();db.destroy();
        }
    }

    @Test
    public void writeQuitReadTest() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 40; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeReplaceQuitReadTest() throws Exception {
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);
        for (int i = 0; i < 40; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        Random rand = new Random();
        for (int i = 0; i < 5; i++) {
            int key = rand.nextInt(40);
            byte[] newValue = bytes(key + "_new_" + VALUE);
            map.put(bytes(key + ""), newValue);
            db.put(bytes(key + ""), newValue);
        }

        db.close();
        db = new DBImpl(dbDirectoryPath, opt);
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            byte[] value = db.get(entry.getKey());
            Assertions.assertArrayEquals(value, entry.getValue());
        }
    }

    @Test
    public void writeDeleteQuitReadTest() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        var keyList = new ArrayList<byte[]>();
        for (int i = 0; i < 100; i++) {
            var key = bytes(i + "");
            var value = bytes(i + "_" + VALUE);
            map.put(key, value);
            db.put(key, value);
            keyList.add(key);
        }

        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            int index = random.nextInt(keyList.size());
            db.delete(keyList.get(index));
            keyList.remove(index);
        }

        db.close();
        db = new DBImpl(dbDirectoryPath, opt);

        for (byte[] bytes : keyList) {
            Assertions.assertArrayEquals(db.get(bytes), map.get(bytes));
        }

        keyList.forEach(map.keySet()::remove);
        // Verify that all keys in the keyList are not present in the database
        for (byte[] bytes : map.keySet()) {
            Assertions.assertNull(db.get(bytes), "Key should not exist in the database: " + new String(bytes));
        }
    }

    @Test
    public void writeQuitWriteQuitReadTest() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 40; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();
        db = new DBImpl(dbDirectoryPath, opt);

        for (int i = 0; i < 40; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();
        db = new DBImpl(dbDirectoryPath, opt);

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void crashRecoveryTest() throws Exception {
        // placeholder for crash recovery test
    }
}