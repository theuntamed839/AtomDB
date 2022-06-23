package Logs;

import Constants.DBConstant;
import Constants.Operations;
import db.DBOptions;
import util.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.LocalDateTime;

import static util.BytesConverter.bytes;

public class FileChannelLogWriter implements LogWriter {
    private FileChannel channel;
    private static String LOG = "LOG";
    private String currentFileName;
    private ByteBuffer byteBuffer;
    private static final byte[] delimiter = bytes(System.lineSeparator());
    private static final int DateTimeLength = 8 + 4;
    private final DBOptions dbOptions;

    public FileChannelLogWriter(DBOptions options) throws IOException {
        byteBuffer = ByteBuffer.allocate(DBConstant.INITIAL_BUFFER_SIZE);
        dbOptions = options;
        createLogFile();
    }

    public void deleteAndCreateNewLogFile() throws IOException {
        deleteLogFile();
        createLogFile();
    }

    private void deleteLogFile() throws IOException {
        closeLogFile();
        if (!new File(dbOptions.getDBfolder() +
                File.separator + currentFileName).delete()) {
            throw new IOException("Log file not deleted");
        }
    }

    private void createLogFile() throws IOException {
        currentFileName = LOG + "-" + Instant.now().toString()
                .replace(':', '_');

        channel = new RandomAccessFile(dbOptions.getDBfolder() +
                File.separator + currentFileName,
                "rw").getChannel();
        channel.force(false);
    }

    @Override
    public String getCurrentFileName() {
        return currentFileName;
    }

    public void closeLogFile() throws IOException {
        channel.close();
    }

    @Override
    public void logOP(byte[] key, byte[] value, Operations operations) throws Exception {
        int length = key.length + value.length + delimiter.length * 2
                + DateTimeLength + Operations.bytesLength() + Long.BYTES * 3;

        byteBuffer = Util.getExtendedBufferIfNeeded(length, byteBuffer);
        byteBuffer.clear();

        LogBlock.write(channel, new LogBlock(
                Instant.now(),
                operations,
                key,
                value
        ), byteBuffer);
    }

    @Override
    public void close() throws IOException {
        closeLogFile();
    }
}
