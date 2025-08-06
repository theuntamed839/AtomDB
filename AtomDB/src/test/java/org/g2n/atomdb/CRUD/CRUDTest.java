package org.g2n.atomdb.CRUD;

import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class CRUDTest {
    public static final int TOTAL = 100000;
    private DB db;
    private DbOptions opt;
    private Path dbPath;

    protected abstract Path getDBPath() throws IOException;
    protected abstract boolean shouldDisableMMap();
    protected void close() throws IOException {
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @BeforeEach
    public void setUp() throws Exception {
        opt = new DbOptions();
        if (shouldDisableMMap()) {
            opt.disallowUseOfMMap();
        }
        dbPath = getDBPath();
        db = new DBImpl(dbPath, opt);
    }

    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("Closing DB and cleaning up resources");
        if (db != null) {
            db.close();
            db.destroy();
        }
        close();
    }

    @Test
    public void testSequentialReadForward() throws Exception {
        var entries = fillDB(TOTAL, db, 1234567890);
        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testSequentialReadBackward() throws Exception {
        var entries = fillDB(TOTAL, db, 1234567890);

        List<byte[]> keys = new ArrayList<>(entries.keySet());
        for (int i = keys.size() - 1; i >= 0; i--) {
            byte[] key = keys.get(i);
            Assertions.assertArrayEquals(db.get(key), entries.get(key));
        }
    }

    @Test
    public void testRandomRead() throws Exception {
        var entries = fillDB(TOTAL, db, 1234567890);

        List<byte[]> keys = new ArrayList<>(entries.keySet());
        Collections.shuffle(keys);
        for (byte[] key : keys) {
            Assertions.assertArrayEquals(db.get(key), entries.get(key));
        }
    }

    @Test
    public void testConcurrentReadWriteSimultaneously() throws Exception {
        var safeEntries = new ConcurrentSkipListMap<byte[], byte[]>(Arrays::compare);
        var writing = new AtomicBoolean(true);
        int writerCount = 4;
        int readerCount = 4;

        ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount);
        Runnable writer = () -> {
            try {
                var rand = new Random();
                for (int i = 0; i < 25000; i++) {
                    var key = new byte[50];
                    var value = new byte[500];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    db.put(key, value);
                    safeEntries.put(key, value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable reader = () -> {
            try {
                while (writing.get()) {
                    var list = new ArrayList<>(safeEntries.keySet());
                    Collections.shuffle(list);
                    for (byte[] key : list) {
                        Assertions.assertArrayEquals(db.get(key), safeEntries.get(key));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        List<Future<?>> writers = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            writers.add(executor.submit(writer));
            executor.submit(reader);
        }

        // Wait for writers to finish
        for (Future<?> f : writers) {
            f.get();
        }
        writing.set(false); // Signal readers to stop

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        for (Map.Entry<byte[], byte[]> entry : safeEntries.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testConcurrentReadWriteDeleteSimultaneously() throws Exception {
        var safeEntries = new ConcurrentSkipListMap<byte[], byte[]>(Arrays::compare);
        var writing = new AtomicBoolean(true);
        int writerCount = 4;
        int readerCount = 4;
        int deleteCount = 2;
        var deleted = new byte[0];

        ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount + deleteCount);
        Runnable writer = () -> {
            try {
                var rand = new Random(123);
                for (int i = 0; i < 25000; i++) {
                    var key = new byte[50];
                    var value = new byte[500];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    db.put(key, value);
                    safeEntries.put(key, value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable reader = () -> {
            try {
                while (writing.get()) {
                    var list = new ArrayList<>(safeEntries.keySet());
                    Collections.shuffle(list);
                    for (byte[] key : list) {
                        if (safeEntries.get(key) == deleted) {
                            Assertions.assertNull(db.get(key), "Expected null for deleted key: " + Arrays.toString(key));
                        }else {
                            Assertions.assertArrayEquals(db.get(key), safeEntries.get(key));
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable delete = () -> {
            try {
                var rand = new Random(1234);
                while (writing.get()) {
                    var list = new ArrayList<>(safeEntries.keySet());
                    byte[] key = list.get(rand.nextInt(list.size()));
                    db.delete(key);
                    safeEntries.put(key, deleted);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        List<Future<?>> writers = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            writers.add(executor.submit(writer));
            executor.submit(reader);
            executor.submit(delete);
        }


        // Wait for writers to finish
        for (Future<?> f : writers) {
            f.get();
        }
        writing.set(false); // Signal readers to stop

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        executor.close();

        for (Map.Entry<byte[], byte[]> entry : safeEntries.entrySet()) {
            if (entry.getValue() == deleted) {
                Assertions.assertNull(db.get(entry.getKey()), "Expected null for deleted key: " + Arrays.toString(entry.getKey()));
            }else {
                Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue(),
                        "Expected value for key: " + Arrays.toString(entry.getValue()));
            }
        }
    }

    @Test
    public void testConcurrentWrite() throws Exception {
        int threadCount = 5;
        int entriesPerThread = 10000;
        Map<byte[], byte[]> allEntries;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<Map<byte[], byte[]>>> futures = new ArrayList<>();

            // Concurrent writes
            for (long i = 0; i < threadCount; i++) {
                long threadId = i;
                futures.add(executor.submit(() -> fillDB(entriesPerThread, db, threadId)));
            }

            // Merge all data into a single list
            allEntries = new HashMap<>();
            for (Future<Map<byte[], byte[]>> future : futures) {
                allEntries.putAll(future.get());
            }

            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        for (Map.Entry<byte[], byte[]> entry : allEntries.entrySet()) {
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testConcurrentRead() throws Exception {
        int threadCount = 5;
        int entriesPerThread = 50000;
        var entries = fillDB(entriesPerThread, db, 1234567890L);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            for (long i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    var kvs = new ArrayList<>(((Map<byte[], byte[]>) entries).entrySet());
                    Collections.shuffle(kvs);
                    for (Map.Entry<byte[], byte[]> entry : kvs) {
                        try {
                            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPut_overwritesExistingKey_returnsLatestValue() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.put("key1".getBytes(), "value2".getBytes());
        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    @Test
    public void testPut_overwritesKey_persistsAcrossDBReopen() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.put("key1".getBytes(), "value2".getBytes());
        db.close();
        db = new DBImpl(dbPath, opt); // Reopen the DB
        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    @Test
    public void testPut_overwritesKeyAfterReopen_returnsLatestValue() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.close();
        db = new DBImpl(dbPath, opt); // Reopen the DB
        db.put("key1".getBytes(), "value2".getBytes());
        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    @Test
    public void testPut_overwritesKey_valuePersistedAfterFlushToSST() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.put("key1".getBytes(), "value2".getBytes());

        var rand = new Random();
        while (true) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);

            db.put(key, value);
            if (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("sst"))) {
                break;
            }
        }

        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    @Test
    public void testPut_overwritesKey_multipleSSTs_returnsLatestValue() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put("key1".getBytes(), "value2".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");
        db.put("asdasda".getBytes(), "asdasd".getBytes());

        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    @Test
    public void testPut_overwritesKey_returnsLatestValueAfterCompaction() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put("key1".getBytes(), "value2".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        // meaning compaction has happened and the old SST files are removed.
        while (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_1.sst")) ||
                Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }

        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    @Test
    public void testPut_overwritesKey_returnsLatestValueAfterCompactionAndReopen() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put("key1".getBytes(), "value2".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        // meaning compaction has happened and the old SST files are removed.
        while (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_1.sst")) ||
                Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }
        db.close();
        db = new DBImpl(dbPath, opt); // Reopen the DB

        byte[] value = db.get("key1".getBytes());
        assert new String(value).equals("value2");
    }

    // deletion
    @Test
    public void testDelete_removesKey_returnsNull() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.delete("key1".getBytes());
        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }

    @Test
    public void testDelete_keyRemainsDeletedAfterReopen() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.delete("key1".getBytes());
        db.close();
        db = new DBImpl(dbPath, opt); // Reopen the DB
        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }

    @Test
    public void testDelete_keyDeletedAfterReopen_returnsNull() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.close();
        db = new DBImpl(dbPath, opt); // Reopen the DB
        db.delete("key1".getBytes());
        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }

    @Test
    public void testDelete_keyDeletedBeforeFlush_remainsDeletedInSST() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        db.delete("key1".getBytes());

        var rand = new Random();
        while (true) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);

            db.put(key, value);
            if (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("sst"))) {
                break;
            }
        }

        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }


    @Test
    public void testDelete_keyDeletedInNewerSST_returnsNull() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.delete("key1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");
        db.put("asdasda".getBytes(), "asdasd".getBytes());

        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }

    @Test
    public void testDelete_keyDeletedBeforeCompaction_returnsNull() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.delete("key1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        // meaning compaction has happened and the old SST files are removed.
        while (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_1.sst")) ||
                Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }

        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }

    @Test
    public void testDelete_keyDeletedBeforeCompaction_returnsNullAfterReopen() throws Exception {
        db.put("key1".getBytes(), "value1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.delete("key1".getBytes());
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        // meaning compaction has happened and the old SST files are removed.
        while (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_1.sst")) ||
                Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains("SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }
        db.close();
        db = new DBImpl(dbPath, opt); // Reopen the DB

        byte[] value = db.get("key1".getBytes());
        assert value == null;
    }


    private void fillDBUntilSSTFileAppears(DB db, String substring) throws Exception {
        var rand = new Random();
        while (true) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);

            db.put(key, value);
            if (Files.list(dbPath.resolve("ATOM_DB")).anyMatch(path -> path.getFileName().toString().contains(substring))) {
                break;
            }
        }
    }

    private void writeRandomKeyValuePairs(DB db, int count) throws Exception {
        var rand = new Random();
        for (int i = 0; i < count; i++) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);
            db.put(key, value);
        }
    }

    private HashMap<byte[], byte[]> fillDB(int numberOfKV, DB db, long seed) throws Exception {
        var entries = new HashMap<byte[], byte[]>();
        var random = new Random(seed);
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

