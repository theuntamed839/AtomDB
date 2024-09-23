package Logs;


import db.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LogReader implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(LogReader.class);
    private final Reader reader;

    public LogReader(File oldlogFile) throws IOException {
        this.reader = new MMapFileReaderPartialMapActAsChannel(oldlogFile);
    }

    public void construct(DB db) throws Exception {
        long fileSize = reader.fileSize();
        LogBlock block;
        for (long i = 0; i < fileSize; i += block.getTotalBytesRequiredForLogBlock()) {
            reader.setPosition(i);
            try {
                block = LogBlock.read(reader);
                switch (block.getOperations()) {
                    case WRITE -> db.put(block.getKey(), block.getValue());
                    case DELETE -> db.delete(block.getKey());
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Corrupted log data");
                break;
            }
        }
        reader.close();
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
