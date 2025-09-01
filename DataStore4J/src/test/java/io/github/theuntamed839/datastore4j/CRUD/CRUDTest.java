package io.github.theuntamed839.datastore4j.CRUD;

import io.github.theuntamed839.datastore4j.db.DB;
import io.github.theuntamed839.datastore4j.db.DataStore4J;
import io.github.theuntamed839.datastore4j.db.DbOptions;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static io.github.theuntamed839.datastore4j.util.BytesConverter.bytes;

public abstract class CRUDTest {
    public static final int TOTAL = 100000;
    private DB db;
    private DbOptions opt;
    private Path dbPath;

    protected abstract Path getDBPath() throws IOException;
    protected abstract boolean shouldDisableMMap();
    protected void close() throws IOException {
        try (Stream<Path> stream = Files.walk(dbPath)) {
            stream.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to delete: " + path, e);
                    }
                });
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        opt = new DbOptions();
        if (shouldDisableMMap()) {
            opt.disallowUseOfMMap();
        }
        dbPath = getDBPath();
        db = new DataStore4J(dbPath, opt);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();
            db.destroy();
            db = null;
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
            executor.execute(reader);
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
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testConcurrentReadWriteDeleteSimultaneously() throws Exception {
        var safeEntries = new ConcurrentSkipListMap<byte[], byte[]>(Arrays::compare);
        final ReentrantLock[] stripes = new ReentrantLock[1024];
        for (int i = 0; i < stripes.length; i++) {
            stripes[i] = new ReentrantLock();
        }
        var writing = new AtomicBoolean(true);
        int writerCount = (int) (Runtime.getRuntime().availableProcessors() * .4);
        int readerCount = (int) (Runtime.getRuntime().availableProcessors() * .4);
        int deleteCount = (int) (Runtime.getRuntime().availableProcessors() * .2);
        var deleted = new byte[0];
        ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount + deleteCount);
        Runnable writer = () -> {
            try {
                var rand = new Random();
                for (int i = 0; i < 25000; i++) {
                    var key = new byte[50];
                    var value = new byte[500];
                    rand.nextBytes(key);
                    rand.nextBytes(value);

                    var lock = stripes[Math.abs(Arrays.hashCode(key) % stripes.length)];
                    lock.lock();
                    try {
                        db.put(key, value);
                        safeEntries.put(key, value);
                    }finally {
                        lock.unlock();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable delete = () -> {
            try {
                var rand = new Random();
                while (writing.get()) {
                    var keyArray = safeEntries.keySet().toArray();

                    if (keyArray.length == 0) continue; // avoid crash

                    var key = (byte[]) keyArray[rand.nextInt(keyArray.length)];
                    var lock = stripes[Math.abs(Arrays.hashCode(key) % stripes.length)];
                    lock.lock();
                    try {
                        db.delete(key);
                        safeEntries.put(key, deleted);
                    }finally {
                        lock.unlock();
                    }
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
                        var lock = stripes[Math.abs(Arrays.hashCode(key) % stripes.length)];
                        lock.lock();
                        try {
                            if (Arrays.compare(safeEntries.get(key), deleted) == 0) {
                                Assertions.assertNull(db.get(key), "Expected null for deleted key: " + Arrays.toString(key));
                            }else {
                                Assertions.assertArrayEquals(db.get(key), safeEntries.get(key));
                            }
                        }finally {
                            lock.unlock();
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        List<Future<?>> writers = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            writers.add(executor.submit(writer));
        }

        for (int i = 0; i < readerCount; i++) {
            executor.execute(reader);
        }

        for (int i = 0; i < deleteCount; i++) {
            executor.execute(delete);
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
            if (Arrays.compare(entry.getValue(), deleted) == 0) {
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
        int entriesPerThread = 50000;
        Map<byte[], byte[]> allEntries;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<Map<byte[], byte[]>>> futures = new ArrayList<>();

            // Concurrent writes
            for (long i = 0; i < threadCount; i++) {
                long threadId = i;
                futures.add(executor.submit(() -> fillDB(entriesPerThread, db, threadId)));
            }

            // Merge all data into a single list
            allEntries = new TreeMap<>(Arrays::compare);
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
        int threadCount = Runtime.getRuntime().availableProcessors();
        int totalEntries = 5_000_00;
        var entries = fillDB(totalEntries, db, 1234567890L);
        List<Future<?>> readers = new ArrayList<>();

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            for (long i = 0; i < threadCount; i++) {
                readers.add(executor.submit(() -> {
                    var kvs = new ArrayList<>(entries.entrySet());
                    Collections.shuffle(kvs);
                    for (Map.Entry<byte[], byte[]> entry : kvs) {
                        try {
                            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
                        } catch (Exception e) {
                            Assertions.fail("Exception during read: " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }
                }));
            }

            for (Future<?> f : readers) {
                f.get();
            }
            executor.shutdown();
        }
    }

    @Test
    public void testCanRetrieveExistingKV() throws Exception {
        db.put(bytes("key1"), bytes("value1"));

        byte[] value = db.get(bytes("key1"));

        Assertions.assertArrayEquals(bytes("value1"), value);
    }

    @Test
    public void testCanRetrieveExistingKVAfterClosing() throws Exception {
        db.put(bytes("key1"), bytes("value1"));

        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt);

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value1"), value);
    }

    @Test
    public void testReturnsNullForNonExistingKey() throws Exception {
        db.put(bytes("key1"), bytes("value1"));

        byte[] value = db.get(bytes("key2"));

        Assertions.assertNull(value, "Expected null for non-existing key");
    }

    @Test
    public void testReturnsNullForNonExistingKeyAfterClosing() throws Exception {
        db.put(bytes("key1"), bytes("value1"));

        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt);

        byte[] value = db.get(bytes("key2"));
        Assertions.assertNull(value, "Expected null for non-existing key");
    }

    @Test
    public void testCanReadKeysAfterMultipleWritesAfterClosing() throws Exception {
        for (int i = 0; i < 10; i++) {
            db.put(bytes("key" + i), bytes("value" + i));
        }

        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt);

        byte[] value = db.get(bytes("key11"));
        Assertions.assertNull(value, "Expected null for non-existing key");

        value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value1"), value, "Expected value for key1");
    }

    @Test
    public void testCanReadKeysAfterMultipleWrites() throws Exception {
        for (int i = 0; i < 10; i++) {
            db.put(bytes("key" + i), bytes("value" + i));
        }

        byte[] value = db.get(bytes("key11"));
        Assertions.assertNull(value, "Expected null for non-existing key");

        for (int i = 0; i < 10; i++) {
            value = db.get(bytes("key" + i));
            Assertions.assertArrayEquals(bytes("value" + i), value);
        }
    }

    @Test
    public void testCanDeleteAnExistingKey() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.put(bytes("key2"), bytes("value2"));

        db.delete(bytes("key1"));

        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for non-existing key");

        value = db.get(bytes("key3"));
        Assertions.assertNull(value, "Expected null for non-existing key");

        value = db.get(bytes("key2"));
        Assertions.assertArrayEquals(bytes("value2"), value, "Expected value for key2");
    }

    @Test
    public void testCanDeleteAnNonExistingKey() throws Exception {
        db.put(bytes("key1"), bytes("value1"));

        db.delete(bytes("key"));

        byte[] value = db.get(bytes("key"));
        Assertions.assertNull(value, "Expected null for non-existing key");
    }

    @Test
    public void testCanReinsertTheDeletedValue() throws Exception {
        db.put(bytes("key1"), bytes("value1"));

        db.delete(bytes("key1"));

        db.put(bytes("key1"), bytes("newValue1"));

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("newValue1"), value, "Expected value for reinserted key1");
    }


    @Test
    public void testOverwritesExistingKey() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.put(bytes("key1"), bytes("value2"));
        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyAndReadAfterReopen() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.put(bytes("key1"), bytes("value2"));
        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt);
        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyAfterReopen() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt); // Reopen the DB
        db.put(bytes("key1"), bytes("value2"));
        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyPersistedAfterFlushAsSST() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.put(bytes("key1"), bytes("value2"));

        var rand = new Random();
        while (true) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);

            db.put(key, value);
            try (Stream<Path> stream = Files.list(dbPath.resolve("DATASTORE4J"))) {
                if (stream.anyMatch(path -> path.getFileName().toString().contains("sst"))) {
                    break;
                }
            }
        }

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyOnMultipleSSTs() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put(bytes("key1"), bytes("value2"));
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");
        db.put(bytes("foo"), bytes("bar"));

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyReturnsLatestValueAfterCompaction() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put(bytes("key1"), bytes("value2"));
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst")) ||
                Files.exists(dbPath.resolve("DATASTORE4J/SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyReturnsLatestValueAfterCompactionAndReopen() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put(bytes("key1"), bytes("value2"));
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst")) ||
                Files.exists(dbPath.resolve("DATASTORE4J/SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }
        db.close();
        db = new DataStore4J(dbPath, opt);

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value2"), value);
    }

    @Test
    public void testOverwritesKeyReturnsLatestValueAfterCompactionAndNewFileIsAddedToNewLevels() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }

        int currentFileNumber;
        try (var stream = Files.list(dbPath.resolve("DATASTORE4J"))) {
             currentFileNumber = stream
                .filter(path -> path.getFileName().toString().contains(".sst"))
                    .filter(path -> !path.getFileName().toString().split("_")[1].equals("0"))
                    .mapToInt(path -> Integer.parseInt(path.getFileName().toString().split("_")[2].replace(".sst", "")))
                    .max().orElseThrow();
        }

        db.put(bytes("key1"), bytes("newValue1"));
        fillDBUntilSSTFileAppears(db, "SST_0_"+(currentFileNumber + 1)+".sst");

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("newValue1"), value, "Expected value for key1 after compaction and new file addition");
    }

    @Test
    public void testOverwritesKeyReturnsLatestValueAfterCompactionAndNewFileIsAddedToNewLevelsAfterClosing() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }

        int currentFileNumber;
        try (var stream = Files.list(dbPath.resolve("DATASTORE4J"))) {
            currentFileNumber = stream
                    .filter(path -> path.getFileName().toString().contains(".sst"))
                    .filter(path -> !path.getFileName().toString().split("_")[1].equals("0"))
                    .mapToInt(path -> Integer.parseInt(path.getFileName().toString().split("_")[2].replace(".sst", "")))
                    .max().orElseThrow();
        }

        db.put(bytes("key1"), bytes("newValue1"));
        fillDBUntilSSTFileAppears(db, "SST_0_"+(currentFileNumber + 1)+".sst");
        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt); // Reopen the DB

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("newValue1"), value, "Expected value for key1 after compaction and new file addition");
    }

    @Test
    public void testOverwritesKeyReturnsLatestValueAfterCompactionAndAfterClosingNewFileIsAddedAtNewLevels() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }
        int currentFileNumber;
        try (var stream = Files.list(dbPath.resolve("DATASTORE4J"))) {
            currentFileNumber = stream
                .filter(path -> path.getFileName().toString().contains(".sst"))
                    .filter(path -> !path.getFileName().toString().split("_")[1].equals("0"))
                    .mapToInt(path -> Integer.parseInt(path.getFileName().toString().split("_")[2].replace(".sst", "")))
                    .max().orElseThrow();
        }

        db.close();
        db = new DataStore4J(dbPath, opt); // Reopen the DB

        db.put(bytes("key1"), bytes("newValue1"));
        fillDBUntilSSTFileAppears(db, "SST_0_"+(currentFileNumber + 1)+".sst");

        byte[] value = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("newValue1"), value, "Expected value for key1 after compaction and new file addition");
    }

    @Test
    public void testEnsureDbOpensReliably() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.put(bytes("key2"), bytes("value2"));

        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt); // Reopen the DB

        byte[] value1 = db.get(bytes("key1"));
        Assertions.assertArrayEquals(bytes("value1"), value1, "Expected value for key1 after compaction and new file addition");
        byte[] value2 = db.get(bytes("key2"));
        Assertions.assertArrayEquals(bytes("value2"), value2, "Expected value for key1 after compaction and new file addition");
    }

    @Test
    public void testRemovesKeyReturnsNull() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.delete(bytes("key1"));
        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }

    @Test
    public void testKeyRemainsDeletedAfterReopen() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.delete(bytes("key1"));
        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt);
        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }

    @Test
    public void testKeyDeletedAfterReopenReturnsNull() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.close();
        Thread.sleep(100); // In JIMFS, it's too fast. log file created with same timestamp
        db = new DataStore4J(dbPath, opt);
        db.delete(bytes("key1"));
        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }

    @Test
    public void testKeyDeletedBeforeFlushRemainsDeletedInSST() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        db.delete(bytes("key1"));

        var rand = new Random();
        while (true) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);

            db.put(key, value);
            try (Stream<Path> stream = Files.list(dbPath.resolve("DATASTORE4J"))) {
                if (stream.anyMatch(path -> path.getFileName().toString().contains("sst"))) {
                    break;
                }
            }
        }

        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }


    @Test
    public void testKeyDeletedInNewerSSTReturnsNull() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.delete(bytes("key1"));
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");
        db.put(bytes("foo"), bytes("bar"));

        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }

    @Test
    public void testKeyDeletedBeforeCompactionReturnsNull() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.delete(bytes("key1"));
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst")) ||
                Files.exists(dbPath.resolve("DATASTORE4J/SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }

        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }

    @Test
    public void testKeyDeletedBeforeCompactionReturnsNullAfterReopen() throws Exception {
        db.put(bytes("key1"), bytes("value1"));
        fillDBUntilSSTFileAppears(db, "SST_0_1.sst");
        db.delete(bytes("key1"));
        fillDBUntilSSTFileAppears(db, "SST_0_2.sst");

        //this loop end, means that compaction has happened and the old SST files are removed.
        while (Files.exists(dbPath.resolve("DATASTORE4J/SST_0_1.sst")) ||
                Files.exists(dbPath.resolve("DATASTORE4J/SST_0_2.sst"))) {
            writeRandomKeyValuePairs(db, 1000);
        }
        db.close();
        db = new DataStore4J(dbPath, opt);

        byte[] value = db.get(bytes("key1"));
        Assertions.assertNull(value, "Expected null for deleted key");
    }


    private void fillDBUntilSSTFileAppears(DB db, String substring) throws Exception {
        var rand = new Random();
        while (true) {
            var key = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(key);
            var value = new byte[rand.nextInt(10, 500)];
            rand.nextBytes(value);

            db.put(key, value);
            try (Stream<Path> stream = Files.list(dbPath.resolve("DATASTORE4J"))) {
                if (stream.anyMatch(path -> path.getFileName().toString().contains(substring))) {
                    break;
                }
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

    private Map<byte[], byte[]> fillDB(int numberOfKV, DB db, long seed) throws Exception {
        var entries = new TreeMap<byte[], byte[]>(Arrays::compare);
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

