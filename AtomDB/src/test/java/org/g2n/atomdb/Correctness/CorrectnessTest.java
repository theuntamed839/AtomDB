package org.g2n.atomdb.Correctness;

import org.g2n.atomdb.db.DBImpl;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;

/**
 * 1. write records and read sequentially (forward and backward)
 * 2. write records and read randomly (all)
 */
public abstract class CorrectnessTest {
    public static final int TOTAL = 100000;
    DBImpl db;
    private Map<byte[], byte[]> entries;

    protected abstract DBImpl createDB() throws Exception;
    protected abstract void destroy() throws IOException;

    @BeforeEach
    public void setup() throws Exception {
        db = createDB();
        entries = new HashMap<>();
        var random = new Random();
        for (int i = 0; i < TOTAL; i++) {
            var key = new byte[50];
            var value = new byte[500];
            random.nextBytes(key);
            random.nextBytes(value);
            entries.put(key, value);
            db.put(key, value);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        db.close();
        db.destroy();
        destroy();
    }

    @Test
    public void sequentialReadForward() throws Exception {
        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
        System.out.println("Sequential read forward completed successfully.");
    }

    @Test
    public void sequentialReadBackward() throws Exception {
        List<byte[]> keys = new ArrayList<>(entries.keySet());
        for (int i = keys.size() - 1; i >= 0; i--) {
            byte[] key = keys.get(i);
            Assertions.assertArrayEquals(db.get(key), entries.get(key));
        }
        System.out.println("Sequential read backward completed successfully.");
    }

    @Test
    public void randomRead() throws Exception {
        List<byte[]> keys = new ArrayList<>(entries.keySet());
        Collections.shuffle(keys);
        for (byte[] key : keys) {
            Assertions.assertArrayEquals(db.get(key), entries.get(key));
        }
        System.out.println("Random read completed successfully.");
    }
}

