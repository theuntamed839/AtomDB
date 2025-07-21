package org.g2n.atomdb.correctness;

import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 1. write records and read sequentially (forward and backward)
 * 2. write records and read randomly (all)
 */
public abstract class CorrectnessTest {
    public static final int TOTAL = 100000;

    protected abstract DBImpl createDB() throws Exception;
    protected abstract void destroy(DB db) throws Exception;

    @Test
    public void sequentialReadForward() throws Exception {
        DB db = createDB();
        var entries = fillDB(TOTAL, db);
        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }

        destroy(db);
    }

    @Test
    public void sequentialReadBackward() throws Exception {
        DB db = createDB();
        var entries = fillDB(TOTAL, db);

        List<byte[]> keys = new ArrayList<>(entries.keySet());
        for (int i = keys.size() - 1; i >= 0; i--) {
            byte[] key = keys.get(i);
            Assertions.assertArrayEquals(db.get(key), entries.get(key));
        }

        destroy(db);
    }

    @Test
    public void randomRead() throws Exception {
        DB db = createDB();
        var entries = fillDB(TOTAL, db);

        List<byte[]> keys = new ArrayList<>(entries.keySet());
        Collections.shuffle(keys);
        for (byte[] key : keys) {
            Assertions.assertArrayEquals(db.get(key), entries.get(key));
        }

        destroy(db);
    }

    @Test
    public void concurrentReadWrite() throws Exception {
        DB db = createDB();
        var entries = fillDB(TOTAL, db);
        var safeEntries = new ConcurrentHashMap<>(entries);
        var writing = new AtomicBoolean(true);
        Runnable writer = () -> {
            try {
                for (int i = 0; i < 100000; i++) {
                    var key = new byte[50];
                    var value = new byte[500];
                    new Random().nextBytes(key);
                    new Random().nextBytes(value);
                    db.put(key, value);
                    safeEntries.put(key, value);
                }
                writing.set(false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable reader = () -> {
            try {
                while(!writing.get()) {
                    for (byte[] key : safeEntries.keySet()) {
                        Assertions.assertArrayEquals(db.get(key), safeEntries.get(key));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };


        Thread writerThread = new Thread(writer);
        Thread readerThread = new Thread(reader);
        writerThread.start();
        readerThread.start();
        writerThread.join();
        readerThread.join();

        for (Map.Entry<byte[], byte[]> entry : safeEntries.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }

        destroy(db);
    }

    private HashMap<byte[], byte[]> fillDB(int numberOfKV, DB db) throws Exception {
        var entries = new HashMap<byte[], byte[]>();
        var random = new Random();
        for (int i = 0; i < numberOfKV; i++) {
            var key = new byte[50];
            var value = new byte[500];
            random.nextBytes(key);
            random.nextBytes(value);
            entries.put(key, value);
            db.put(key, value);
        }
        return entries;
    }
}

