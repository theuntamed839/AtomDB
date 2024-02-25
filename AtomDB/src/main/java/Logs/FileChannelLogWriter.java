package Logs;

import Constants.DBConstant;
import Constants.Operations;
import util.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;

import static util.BytesConverter.bytes;

public class FileChannelLogWriter implements LogWriter {
    private FileChannel channel;
    private static String LOG = "LOG";
    private String logFileName;
    private ByteBuffer byteBuffer;
    private static final byte[] delimiter = bytes(System.lineSeparator());
    private static final int InstantClassStorageLength = 8 + 4; // epoch second and milli.
    private final String dbFolderPath;

    public FileChannelLogWriter(String dbFolderPath) throws IOException {
        this.byteBuffer = ByteBuffer.allocate(DBConstant.INITIAL_BUFFER_SIZE);
        this.dbFolderPath = dbFolderPath;
        createLogFile();
    }

    private void createLogFile() throws IOException {
        //todo need to make this common somewhere else or make it static. think
        logFileName = LOG + "-" + Instant.now().toString()
                .replace(':', '_');
        channel = new RandomAccessFile(dbFolderPath +
                File.separator + logFileName,
                "rw").getChannel();
        channel.force(true);
    }

    @Override
    public void logOP(byte[] key, byte[] value, Operations operations) throws Exception {
        LogBlock block = new LogBlock(
                Instant.now(),
                operations,
                key,
                value
        );
        byteBuffer = Util.getExtendedBufferIfNeeded((int)block.totalBytesRead() + delimiter.length * 2, byteBuffer);
        byteBuffer.clear();
        LogBlock.write(channel, block, byteBuffer);
    }

    @Override
    public String getLogFileName() {
        return logFileName;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
