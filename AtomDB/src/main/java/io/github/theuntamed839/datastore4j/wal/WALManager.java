package io.github.theuntamed839.datastore4j.wal;

import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.constants.Operations;
import io.github.theuntamed839.datastore4j.db.DB;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.ExpandingByteBuffer;
import io.github.theuntamed839.datastore4j.db.KVUnit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.locks.StampedLock;
import java.lang.System.Logger;

public class WALManager implements AutoCloseable {
    private static final Logger logger = System.getLogger(WALManager.class.getName());
    private static final String LOG_PREFIX = "LOG_PREFIX";
    private final WalEncoderDecoder walEncoderDecoder;
    private LogWriter writer;
    private final Path logDirPath;
    private final DbComponentProvider componentProvider;
    private Path currLogPath;
    private final ExpandingByteBuffer buffer = new ExpandingByteBuffer();
    private final StampedLock lock = new StampedLock();

    public WALManager(Path dbPath, DbComponentProvider componentProvider) throws IOException {
        this.logDirPath = dbPath.resolve("Logs");
        this.componentProvider = componentProvider;
        Files.createDirectories(logDirPath);
        startNewLog();
        this.walEncoderDecoder = new WalEncoderDecoder();
    }

    public void restore(DB db) throws Exception {
        logger.log(Logger.Level.INFO, String.format("Trying to restore database from log files: %s", logDirPath));
        var oldLog = findLastModifiedLog();
        if (oldLog != null) {
            try (var reader = componentProvider.getIOReader(oldLog)) {
                while(reader.hasRemaining()) {
                    LogBlock logBlock = walEncoderDecoder.decoder(reader);
                    switch (logBlock.operation()) {
                        case WRITE -> db.put(logBlock.kvUnit().getKey(), logBlock.kvUnit().getValue());
                        case DELETE -> db.delete(logBlock.kvUnit().getKey());
                    }
                }
            } catch (Exception e) {
                logger.log(Logger.Level.ERROR, String.format("Failed to restore from log file: %s", oldLog));
                throw e;
            }
            Files.delete(oldLog);
        } else {
            logger.log(Logger.Level.INFO, "No previous logs found. Starting fresh database.");
        }
    }

    public void log(Operations operations, KVUnit kvUnit) throws IOException {
        long stamp = lock.writeLock();
        try {
            buffer.clear();
            walEncoderDecoder.encode(buffer, operations, kvUnit);
            buffer.flip();
            writer.write(buffer.getBuffer());
        }finally {
            lock.unlockWrite(stamp);
        }
    }

    public void rotateLog() throws Exception {
        long stamp = lock.writeLock();
        try {
            closeCurrentLog();
            startNewLog();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private Path findLastModifiedLog() throws IOException {
        try (var stream = Files.walk(this.logDirPath)) {
            return stream
                    .filter(this::isLogFile)
                    .filter(log -> !log.equals(currLogPath))
                    .min(this::compareLogFiles) // Find the oldest log file.
                    .orElse(null);
        }
    }

    private boolean isLogFile(Path file) {
        return file.getFileName().toString().contains(LOG_PREFIX) && !file.getFileName().toString().contains(DBConstant.OBSOLETE);
    }

    private Instant extractTimestamp(Path file) {
        String timestampStr = file.getFileName().toString().replace(LOG_PREFIX + "-", "");
        return Instant.ofEpochMilli(Long.parseLong(timestampStr));
    }

    private int compareLogFiles(Path file1, Path file2) {
        Instant time1 = extractTimestamp(file1);
        Instant time2 = extractTimestamp(file2);
        return time1.compareTo(time2);
    }

    private void startNewLog() throws IOException {
        if (currLogPath != null) {
            throw new IllegalStateException("A log file is already open");
        }
        currLogPath = generateNewLogFile();
        writer = new FileChannelWriter(currLogPath);
    }

    private Path generateNewLogFile() throws IOException {
        Path path = this.logDirPath.resolve(LOG_PREFIX + "-" + Instant.now().toEpochMilli());
        Files.createFile(path);
        return path;
    }

    private void closeCurrentLog() throws Exception {
        writer.close();
        Files.delete(currLogPath);
        currLogPath = null;
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            long stamp = lock.writeLock();
            try {
                writer.close();
            } finally {
                lock.unlockWrite(stamp);
            }
            writer = null;
        }
    }
}
