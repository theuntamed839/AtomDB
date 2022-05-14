import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.util.Arrays;

import static util.BytesConverter.bytes;
public class LogBlock {
    private byte[] key, value;
    private byte[] time;
    private Operations operations;

    private long checksum;
    public LogBlock(byte[] time,  Operations operations, byte[] key, byte[] value) {
        this.time = time;
        this.key = key;
        this.value = value;
        this.operations = operations;
        this.checksum = CheckSum.logBlock(time, operations.value(), key, value);
    }

    // total bytes read for extraction of this block
    //
    public long totalBytesRead() {
       return time.length +
               SizeOf.OperationsLength +
               SizeOf.LongLength + // key length
               key.length +
               SizeOf.LongLength + // value length
               value.length +
               SizeOf.LongLength; // checksum
    }

    public long getChecksum() {
        return checksum;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public byte[] getTime() {
        return time;
    }

    public Operations getOperations() {
        return operations;
    }

    public static void write(FileChannel channel, LogBlock block, ByteBuffer buffer) throws IOException {

        long checksum = CheckSum.logBlock(block.time, block.operations.value(), block.key, block.value);
        buffer.clear();
        buffer.put(block.time)
                .put(block.operations.value())
                .putLong(block.key.length)
                .put(block.key)
                .putLong(block.value.length)
                .put(block.value)
                .putLong(checksum);
        buffer.flip();
        channel.write(buffer);
    }

    /*
    *   todo
    *    maybe we can convert this into one read and then parse
    * */
    public static LogBlock read(FileChannel channel, ByteBuffer buffer) throws Exception {
        // time readd
        readNextArrayWithN(channel, buffer, SizeOf.DateTimeLength);
        byte[] time = getByteArray(buffer, SizeOf.DateTimeLength);

        // operation read
        readNextArrayWithN(channel, buffer, SizeOf.OperationsLength);
        Operations op = Operations.getOP(
                getByteArray(buffer, SizeOf.OperationsLength));

        // key read
        int length = readNextArray(channel, buffer);
        byte[] key = getByteArray(buffer, length);

        //value read
        length = readNextArray(channel, buffer);
        byte[] value = getByteArray(buffer, length);

        // checksum read
        readNextArrayWithN(channel, buffer, SizeOf.LongLength);
        long checksum = buffer.getLong();

        LogBlock logBlock = new LogBlock(time, op, key, value);

        if (checksum != logBlock.checksum)
            throw new Exception("checksum error for logBlock=" + logBlock);
        System.out.println("read block " + logBlock);
        return logBlock;
    }

    private static int readNextArray(FileChannel channel, ByteBuffer buffer) throws IOException {
        buffer.clear();
        buffer.limit(SizeOf.LongLength);
        channel.read(buffer);
        buffer.flip();
        int length = (int) buffer.getLong();
        buffer.clear();
        readNextArrayWithN(channel, buffer, length);
        return length;
    }

    private static void readNextArrayWithN(FileChannel channel, ByteBuffer buffer, int len) throws IOException {
        buffer.clear();
        buffer.limit(len);
        channel.read(buffer);
        buffer.flip();
    }

    private static byte[] getByteArray(ByteBuffer buffer, int len) throws Exception {
        if (buffer.limit() == 0) throw new Exception("ByteBuffer empty");
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public String toString() {
        return "LogBlock{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", time=" + Arrays.toString(time) +
                ", operations=" + operations +
                ", checksum=" + checksum +
                '}';
    }
}
