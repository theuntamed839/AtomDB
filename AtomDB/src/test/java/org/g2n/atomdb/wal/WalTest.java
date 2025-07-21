package org.g2n.atomdb.wal;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.g2n.atomdb.util.BytesConverter.bytes;

public class WalTest {
    public static final String VALUE = "value".repeat(50);
    DbOptions opt = new DbOptions();
    DBImpl db;
    private Path dbDirectoryPath;
    private FileSystem jimfs;

    @BeforeEach
    public void setup() throws Exception {
        jimfs = Jimfs.newFileSystem(Configuration.windows());
        dbDirectoryPath = jimfs.getPath("JIMFS_DIR");
        opt.disallowUseOfMMap(); // so that we can use the jimfs file system
        db = new DBImpl(dbDirectoryPath, opt);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();db.destroy();
        }
        if (jimfs != null) {
            jimfs.close();
        }
    }

    @Test
    public void writeQuitReadTest() throws Exception {
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);
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
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);
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
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);
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
    public void writeEnoughToGenerateSSTThenRead() throws Exception {
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);

        boolean isSSTGenerated = false;
        // purposely one extra iteration
        for (int i = 0; !isSSTGenerated; i++) {
            isSSTGenerated = Files.walk(dbDirectoryPath).anyMatch(path -> path.getFileName().toString().contains(".sst"));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();
        db = new DBImpl(dbDirectoryPath, opt);
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeEnoughToGenerateSSTThenReplaceAndDeleteEntryAndRead() throws Exception {
        var map = new TreeMap<byte[], byte[]>(Arrays::compare);

        boolean isSSTGenerated = false;
        for (int i = 0; !isSSTGenerated; i++) {
            isSSTGenerated = Files.walk(dbDirectoryPath).anyMatch(path -> path.getFileName().toString().contains(".sst"));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        // replace
        db.put(bytes(0 + ""), bytes("VALUE"));
        map.put(bytes(0 + ""), bytes("VALUE"));

        // delete
        db.delete(bytes(1 + ""));
        map.put(bytes(1 + ""), null);

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