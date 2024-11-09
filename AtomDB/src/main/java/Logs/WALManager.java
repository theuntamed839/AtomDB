package Logs;

import Constants.DBConstant;
import Constants.Operations;
import db.DB;
import db.ExpandingByteBuffer;
import db.KVUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.Arrays;

public class WALManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
    private static final String LOG_PREFIX = "LOG_PREFIX";
    private static final String OBSOLETE_PREFIX = "OBSOLETE";
    private Writer writer;
    private final String directoryPath;
    private File currentLogFile;
    private static final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public WALManager(String directoryPath) throws IOException {
        this.directoryPath = directoryPath;
        startNewLog();
    }

    public void restore(DB db) throws Exception {
        File oldLogFile = findLastModifiedLog();
        if (oldLogFile != null) {
            try (
                    var randomAccessFile = new RandomAccessFile(oldLogFile, "r");
                    var channel = randomAccessFile.getChannel()
                    ) {
                var mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                while(mmap.position() != channel.size()){
                    var operation = Operations.getOperation(mmap.get());
                    var kvunit = KVUnit.read(mmap);
                    switch (operation) {
                        case WRITE -> db.put(kvunit.getKey(), kvunit.getValue());
                        case DELETE -> db.delete(kvunit.getKey());
                    }
                }
                deleteOrMarkAsObsolete(oldLogFile);
            } catch (Exception e) {
                logger.error("Failed to restore from log file: {}", oldLogFile.getName(), e);
                throw e;
            }
        } else {
            logger.info("No previous logs found. Starting fresh database.");
        }
    }

    private File findLastModifiedLog() {
        File[] files = new File(directoryPath).listFiles();
        if (files == null) {
            logger.warn("No files found in directory: {}", directoryPath);
            return null;
        }
        return Arrays.stream(files)
                .filter(this::isLogFile)
                .filter(log -> !log.getName().equals(currentLogFile.getName()))
                .max((log1, log2) -> extractTimestamp(log1).compareTo(extractTimestamp(log2)))
                .orElse(null);
    }

    private boolean isLogFile(File file) {
        return file.getName().contains(LOG_PREFIX) && !file.getName().contains(DBConstant.OBSOLETE);
    }

    private Instant extractTimestamp(File file) {
        String timestampStr = file.getName().replace(LOG_PREFIX + "-", "").replace('_', ':');
        return Instant.parse(timestampStr);
    }

    private void startNewLog() throws IOException {
        if (currentLogFile != null) {
            throw new IllegalStateException("startNewLog called multiple times");
        }
        String logFileName = generateLogFileName();
        currentLogFile = new File(directoryPath + File.separator + logFileName);
        writer = new SynchronizedFileChannelWriter(currentLogFile);

    }

    private String generateLogFileName() {
        return LOG_PREFIX + "-" + Instant.now().toString().replace(':', '_');
    }

    public void log(Operations operations, KVUnit kvUnit) throws IOException {
        var buffer = bufferThreadLocal.get();
        buffer.clear();
        buffer.put(operations.value());
        kvUnit.uploadKV(buffer);
        buffer.flip();
        writer.write(buffer.getBuffer());
    }

    public void rotateLog() throws Exception {
        closeCurrentLog();
        startNewLog();
    }

    private void closeCurrentLog() throws Exception {
        writer.close();
        deleteOrMarkAsObsolete(currentLogFile);
        currentLogFile = null;
    }

    private void deleteOrMarkAsObsolete(File file) {
        if (!file.delete()) {
            var markingObsolete = new File(file.getAbsolutePath() + "_" + OBSOLETE_PREFIX);
            if (!file.renameTo(markingObsolete)) {
                logger.error("Failed to mark file as obsolete: {}", file.getAbsolutePath());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }
}
