package Logs;

import Constants.DBConstant;
import db.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static util.BytesConverter.bytes;
/*
* todo
*  log reader and log writer both have lot of common things
*  try to reduce the duplication of code
* */

public class FileChannelLogReader implements LogReader{
    private static final Logger logger = LoggerFactory.getLogger(FileChannelLogReader.class);
    public static final String READ_MODE = "r";
    private static String LOG = "LOG";
    private File logFile = null;
    private ByteBuffer byteBuffer;
    private static final byte[] delimiter = bytes(System.lineSeparator());
    private static final int DateTimeLength = bytes(LocalDateTime.now().toString()).length;

    public FileChannelLogReader(File dbFolder, String currentLogFile) throws Exception {
        List<File> logFiles = new ArrayList<>();
        for (File file : Objects.requireNonNull(dbFolder.listFiles())) {
            if (file.getName().contains(LOG) && !file.getName().equals(currentLogFile)) {
                logFiles.add(file);
            }
        }

        if (logFiles.size() == 0) {
            logger.debug("Didn't find any log");
        }

        logFile = getTheOldestLogFile(logFiles);
        byteBuffer = ByteBuffer.allocate(DBConstant.INITIAL_BUFFER_SIZE);
    }

    // todo
    // need to move this method to the class where the creation of
    // log name is done
    private File getTheOldestLogFile(List<File> logFiles) throws Exception {
        Instant latest = Instant.MIN;
        File foundLog = null;
        for (File file : logFiles) {
            String log = file.getName().replace(LOG + "-", "")
                    .replace('_', ':');
            Instant dateTime = Instant.parse(log);

            if (latest.isBefore(dateTime)) {
                latest = dateTime;
                foundLog = file;
            }
        }

        if (foundLog == null) {
            throw new Exception("no log file found");
        }

        if (logFiles.size() > 2) {
            logger.debug("Multiple Log File found " + logFiles);
            logger.debug("using " + (foundLog.getName()));
        }

        return foundLog;
    }

    @Override
    public void readWAL(DB db) throws Exception {
        try (RandomAccessFile reader =
                     new RandomAccessFile(logFile, READ_MODE);
             FileChannel channel = reader.getChannel();
             FileLock lock = channel.lock()
        ) {
            LogBlock block = null;
            for (long i = 0; i < channel.size(); i += block.totalBytesRead()) {
                block = LogBlock.read(channel, byteBuffer);
                switch (block.getOperations()) {
                    case WRITE -> db.put(block.getKey(), block.getValue());
                    case DELETE -> db.delete(block.getKey());
                    // todo need to improve, in sst for delete operation. it need to file if that element exists before storing
                    // if the element not exists and there was a delete operation then do not store it.
                    case UPDATE -> {
                        db.put(block.getKey(), block.getValue());
                        // todo below code is wrong, but will work for current time
//                        if (db.put(block.getKey(), block.getValue()) == null) {
//                            throw new Exception("updation before any key entered");
//                        }
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
        logger.debug("deleting log="+logFile.getName());
        if (!logFile.delete()) {
            throw new RuntimeException(logFile + " not deleted");
        }
    }
}
