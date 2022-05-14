import org.xerial.snappy.PureJavaCrc32C;
import util.BytesConverter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static util.BytesConverter.bytes;

public class FileChannelLogWriter implements LogWriter {
    private FileChannel channel;
    private static String LOG = "LOG";
    private String currentFileName;
    private ByteBuffer byteBuffer;
    //todo
    // this can be removed and bytebuffer api can be used to get the capacity
    private int currentByteBufferLimit = 4 * SizeOf.MBinBytes;
    private static final int delimiterLength = System.lineSeparator().getBytes(StandardCharsets.UTF_8).length; //1
    private static final byte[] delimiter = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
    private static final int DateTimeLength = 30;

    private final DBOptions dbOptions;
    private static final PureJavaCrc32C crc32cChecksum = new PureJavaCrc32C();
    public FileChannelLogWriter(DBOptions options) throws IOException {
        byteBuffer = ByteBuffer.allocate(currentByteBufferLimit);
        dbOptions = options;
        refresh();
    }

    public void createNewLog() throws Exception {
        closeFile();
        if (!new File(currentFileName).delete()) {
            throw new Exception("log file not deleted");
        }
        refresh();
    }

    private void refresh() throws IOException {
        currentFileName = dbOptions.getDBfolder() + File.separator +
                LOG + "-" + LocalDateTime.now();
        channel = new RandomAccessFile(currentFileName,
                "rw").getChannel();
        channel.force(false);
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    public void put(byte[] key, byte[] value) throws Exception {
        logWrite(key, value);
    }

    private void logWrite(byte[] key, byte[] value) throws Exception {
        int length = key.length + value.length + delimiterLength * 2 + DateTimeLength;
        if (length > currentByteBufferLimit) {
            setByteBufferLimit(length);
        }
        byteBuffer.clear();
        byteBuffer.put(key);byteBuffer.put(delimiter);
        byteBuffer.put(value);byteBuffer.put(delimiter);
        byteBuffer.flip();
        channel.write(byteBuffer);
        byteBuffer.clear();
    }

    private void setByteBufferLimit(int length) throws Exception {
        if (length > currentByteBufferLimit) {
            throw new Exception("byteBufferLimit attempt shrink");
        }
        byteBuffer = ByteBuffer.allocate(length + 10);
        currentByteBufferLimit = length;
    }

    public void closeFile() throws IOException {
        channel.close();
    }

    @Override
    public void logOP(byte[] key, byte[] value, Operations operations) throws Exception {
        int length = key.length + value.length + delimiterLength * 2 + DateTimeLength;
        extendBufferLimitIfRequired(length);
        byteBuffer.clear();

        LogBlock.write(channel, new LogBlock(
                bytes(LocalDateTime.now().toString()),
                operations,
                key,
                value
        ), byteBuffer);
    }

    @Override
    public void close() throws IOException {
        closeFile();
    }

    private long getCheckSum(byte[] time, byte[] operation, byte[] key, byte[] value) {
        crc32cChecksum.reset();
        crc32cChecksum.update(time);
        crc32cChecksum.update(operation);
        crc32cChecksum.update(key);
        crc32cChecksum.update(value);
        System.out.println(crc32cChecksum.getValue());
        if (crc32cChecksum.getValue() != crc32cChecksum.getValue()) System.out.println("fucke crc32d");
        return crc32cChecksum.getValue();
    }

    private void extendBufferLimitIfRequired(int length) throws Exception {
        if (length > currentByteBufferLimit) {
            setByteBufferLimit(length);
        }
    }
}
