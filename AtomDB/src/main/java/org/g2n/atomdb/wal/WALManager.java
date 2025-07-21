package org.g2n.atomdb.wal;

import org.g2n.atomdb.constants.DBConstant;
import org.g2n.atomdb.constants.Operations;
import org.g2n.atomdb.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class WALManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
    private static final String LOG_PREFIX = "LOG_PREFIX";
    private final WalEncoderDecoder walEncoderDecoder;
    private Writer writer;
    private final Path logDirPath;
    private final DbComponentProvider componentProvider;
    private Path currLogPath;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public WALManager(Path dbPath, DbComponentProvider componentProvider) throws IOException {
        this.logDirPath = dbPath.resolve("Logs");
        this.componentProvider = componentProvider;
        Files.createDirectories(logDirPath);
        startNewLog();
        this.walEncoderDecoder = new WalEncoderDecoder();
    }

    public void restore(DB db) throws Exception {
        logger.info("Restoring database from log files in: {}", logDirPath);
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
                logger.error("Failed to restore from log fileToWrite: {}", oldLog, e);
                throw e;
            }
            Files.delete(oldLog);
        } else {
            logger.info("No previous logs found. Starting fresh database.");
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
        String timestampStr = file.getFileName().toString().replace(LOG_PREFIX + "-", "").replace('_', ':');
        return Instant.parse(timestampStr);
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
        writer = new SynchronizedFileChannelWriter(currLogPath);
    }

    private Path generateNewLogFile() throws IOException {
        Path path = this.logDirPath.resolve(LOG_PREFIX + "-" + Instant.now().toString().replace(':', '_'));
        Files.createFile(path);
        return path;
    }

    public void log(Operations operations, KVUnit kvUnit) throws IOException {
        var buffer = bufferThreadLocal.get();
        buffer.clear();
        walEncoderDecoder.encode(buffer, operations, kvUnit);
        buffer.flip();
        writer.write(buffer.getBuffer());
    }

    public void rotateLog() throws Exception {
        closeCurrentLog();
        startNewLog();
    }

    private void closeCurrentLog() throws Exception {
        writer.close();
        Files.delete(currLogPath);
        currLogPath = null;
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }
}
