package Logs;

import Constants.DBConstant;
import db.DB;
import db.DBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private static String LOG = "LOG";
    private File logFile = null;
    private ByteBuffer byteBuffer;
    private static final byte[] delimiter = bytes(System.lineSeparator());
    private static final int DateTimeLength = bytes(LocalDateTime.now().toString()).length;

    public FileChannelLogReader(DBOptions dbOptions, String currentLogFile) throws Exception {
        var folder = new File(dbOptions.getDBfolder());
        List<File> logFiles = new ArrayList<>();

        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (file.getName().contains(LOG) && !file.getName().equals(currentLogFile)) {
                logFiles.add(file);
            }
        }
        if (logFiles.size() == 0) throw new Exception("No log file found");
        logFile = getLatestLog(logFiles);
        byteBuffer = ByteBuffer.allocate(DBConstant.INITIAL_BUFFER_SIZE);
    }

    // todo
    // need to move this method to the class where the creation of
    // log name is done
    private File getLatestLog(List<File> logFiles) throws Exception {
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

        if (latest.equals(Instant.MIN)) {
            throw new Exception("no log file found");
        }
        if (logFiles.size() > 2) {
            logger.debug("Multiple Log File found " + logFiles);
            logger.debug("using " + (foundLog.getName()));
            System.out.println("Multiple Log File found " + logFiles);
            System.out.println("using " + (foundLog.getName()));
        }

        return foundLog;
    }

    @Override
    public void readWAL(DB db) throws Exception {
        LogBlock current = null;
        try (RandomAccessFile reader =
                     new RandomAccessFile(logFile, "r")) {
            FileChannel channel = reader.getChannel();

            for (long i = 0; i < channel.size(); i += current.totalBytesRead()) {
                current = LogBlock.read(channel, byteBuffer);
                switch (current.getOperations()) {
                    case WRITE -> db.put(current.getKey(), current.getValue());
                    case DELETE -> { // todo need to improve
                        db.delete(current.getKey());
//                        byte[] value = db.get(current.getKey());
//                        if (value != null) {
//                            if (Arrays.compare(value, current.getValue()) == 0) {
//                                db.delete(current.getKey());
//                            } else {
//                                throw new Exception("previous value mismatch for the key");
//                            }
//                        } else {
//                            throw new Exception("key not found in db");
//                        }
                    }

                    case UPDATE -> {
                        db.put(current.getKey(), current.getValue());
                        // todo below code is wrong, but will work for current time
//                        if (db.put(current.getKey(), current.getValue()) == null) {
//                            throw new Exception("updation before any key entered");
//                        }
                    }
                }
            }
            channel.close();

        } catch (Exception e) {
            throw e;
        }
        logger.debug("deleting log="+logFile.getName());
        System.out.println("deleting log="+logFile.getName());
        if (!logFile.delete()) {
            throw new Exception(logFile + " not deleted");
        }
    }
}
