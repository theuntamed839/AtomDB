package Logs;

import Constants.DBConstant;
import Constants.Operations;
import db.DB;
import db.KVUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// todo the string manipulation of Instant should be separated out
public class WALManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
    private Writer writer;
    private final String directoryPath;
    private File currentLogFile;

    public WALManager(String directoryPath) throws IOException {
        this.directoryPath = directoryPath;
        startLog();
    }

    public void restore(DB db) throws Exception {
        File oldlogFile = getOldlogFile();
        if (oldlogFile != null) {
            try (LogReader logReader = new LogReader(oldlogFile)) {
                logReader.construct(db);
                obsoleteFile(oldlogFile);
                return;
            }
        }
        logger.debug("Didn't find any log, seems new db");
    }

    private File getOldlogFile() {
        List<File> logFiles = new ArrayList<>();
        for (File file : Objects.requireNonNull(new File(directoryPath).listFiles())) {
            if (
                    file.getName().contains(DBConstant.LOG) &&
                            !file.getName().contains(DBConstant.OBSOLETE) &&
                            !file.getName().equals(currentLogFile.getName())) {
                logFiles.add(file);
            }
        }
        File fileToRestore = null;
        if (logFiles.size() == 1) {
            fileToRestore = logFiles.getFirst();
        }
        if (logFiles.size() > 1) {
            logger.debug("Multiple valid log file found something suspicious");
            fileToRestore = getTheOldestLogFile(logFiles);
        }
        return fileToRestore;
    }

    private void startLog() throws IOException {
        if (currentLogFile != null) {
            throw new RuntimeException("Multiple time startLog call");
        }
        // "LOG-2024-03-28T10_00_48.264561700Z" format like this
        currentLogFile = FileUtil.createFileAtDirectory(directoryPath,
                "LOG" + "-" + Instant.now().toString().replace(':', '_'));
        writer = new SynchronizedFileChannelWriter(currentLogFile);
    }

    public void log(Operations operations, KVUnit kvUnit) throws IOException {
        writer.write(new LogBlock(operations, kvUnit.getKey(), kvUnit.getValue()).getBytes());
    }

    public void deleteOldLogAndCreateNewLog() throws Exception {
        deleteLogFile();
        startLog();
    }

    private void deleteLogFile() throws Exception {
        writer.close();
        obsoleteFile(currentLogFile);
        currentLogFile = null;
    }

    private void obsoleteFile(File file) {
        // caution, we only make the log file obsolete when the sst is written. scheduler will delete it.
        if (FileUtil.makeFileObsolete(file) == null) {
             logger.error("unable to rename file="+file.getAbsolutePath());
             logger.info("proceeding to delete");
//            if (!file.delete()) {
//                throw new RuntimeException("Cannot delete files");
//            }
            file.deleteOnExit();
        }
    }

    private File getTheOldestLogFile(List<File> logFiles){
        Instant oldFileTime = Instant.MAX;
        File foundLog = null;
        for (File file : logFiles) {
            String log = file.getName().replace(DBConstant.LOG + "-", "")
                    .replace('_', ':');

            Instant createdFileTime = Instant.parse(log);
            if (createdFileTime.isBefore(oldFileTime)) {
                oldFileTime = createdFileTime;
                foundLog = file;
            }
        }

        if (foundLog == null) {
            throw new RuntimeException("no log file found");
        }

        if (logFiles.size() > 1) {
            logger.debug("Multiple Log File found " + logFiles);
            logger.debug("using " + (foundLog.getName()));
        }

        return foundLog;
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
