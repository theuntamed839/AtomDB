import db.DBImpl;
import db.DbOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static util.BytesConverter.bytes;

public class WalTest {
    public static final String VALUE = "value".repeat(50);
    DbOptions opt ;
    DBImpl db;
    private String dbDirectory;

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        dbDirectory = this.getClass().getName() + "_" + Instant.now().getEpochSecond() + "_DB";
        File directory = new File(dbDirectory);
        if (!directory.mkdir()) {
            throw new RuntimeException("Folder cannot be created");
        }
        db = new DBImpl(directory, opt);
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();db.destroy();
    }

    @Test
    public void writeQuitRead10() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 10; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();

        db = new DBImpl(new File(dbDirectory), opt);

        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (i % 10 == 0) System.out.println("hey");
            i++;
            System.out.println(new String(entry.getKey()) + "->" + new String(entry.getValue()));
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeQuitRead100() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 100; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();

        db = new DBImpl(new File(dbDirectory), opt);

        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (i % 10 == 0) System.out.println("hey");
            i++;
            System.out.println(new String(entry.getKey()) + "->" + new String(entry.getValue()));
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeQuitRead1000() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 1000; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();

        db = new DBImpl(new File(dbDirectory), opt);

        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (i % 10 == 0) System.out.println("hey");
            i++;
            System.out.println(new String(entry.getKey()) + "->" + new String(entry.getValue()));
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeQuitRead10000() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 10000; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();

        db = new DBImpl(new File(dbDirectory), opt);

        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (i % 10 == 0) System.out.println("hey");
            i++;
            System.out.println(new String(entry.getKey()) + "->" + new String(entry.getValue()));
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeDeleteQuitRead() throws Exception {
        this.db.close();
        DBImpl db = new DBImpl(new File(dbDirectory), opt);

        var map = new HashMap<byte[], byte[]>();
        var list = new ArrayList<byte[]>();
        for (int i = 0; i < 1000; i++) {
            var key = bytes(i + "");
            var value = bytes(i + "_" + VALUE);
            map.put(key, value);
            db.put(key, value);
            list.add(key);
        }

        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            int index = random.nextInt(list.size());
            db.delete(list.get(index));
            map.remove(list.get(index));
            list.remove(index);
        }

        db.close();
        db = new DBImpl(new File(dbDirectory), opt);

        for (byte[] bytes : list) {
            System.out.println("this"+new String(bytes));
            Assertions.assertArrayEquals(db.get(bytes), map.get(bytes));
        }
        this.db = db;
    }
}