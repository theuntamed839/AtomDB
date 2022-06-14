package Logs;

import Checksum.CheckSum;
import Constants.Operations;
import util.BytesConverter;
import util.SizeOf;
import util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.Arrays;
public class LogBlock {
    private byte[] key, value;
    private Instant time;
    private Operations operations;

    private long checksum;
    public LogBlock(Instant time,  Operations operations, byte[] key, byte[] value) {
        this.time = time;
        this.key = key;
        this.value = value;
        this.operations = operations;
        this.checksum = CheckSum.logBlock(time, operations.value(), key, value);
    }

    public LogBlock(byte[] timeInBytes,  Operations operations, byte[] key, byte[] value) {
        this.time = Instant.ofEpochSecond(
                BytesConverter.toLong(timeInBytes, 0, 8),
                BytesConverter.toInt(timeInBytes, 8, 12)
        );
        this.key = key;
        this.value = value;
        this.operations = operations;
        this.checksum = CheckSum.logBlock(time, operations.value(), key, value);
    }

    // total bytes read for extraction of this block
    //
    public long totalBytesRead() {
       return SizeOf.InstantTimeLength +
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

    public Instant getTime() {
        return time;
    }

    public Operations getOperations() {
        return operations;
    }

    public static void write(FileChannel channel,
                             LogBlock block,
                             ByteBuffer buffer) throws IOException {

        long checksum = CheckSum.logBlock(block.time, block.operations.value(), block.key, block.value);
        buffer.clear();
        // storage
        // storing time
        Util.putTime(block.time, buffer);
        buffer.put(block.operations.value())
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
        byte[] time = getByteArray(channel, buffer, SizeOf.InstantTimeLength);

        // operation read
        Operations op = Operations.getOP(
                getByteArray(channel, buffer, SizeOf.OperationsLength));

        // key read
        // todo need to come up with better name
        byte[] key = getByteArray(channel, buffer);

        //value read
        byte[] value = getByteArray(channel, buffer);

        // checksum read
        readNextArrayWithN(channel, buffer, SizeOf.LongLength);
        long checksum = buffer.getLong();

        LogBlock logBlock = new LogBlock(time, op, key, value);

        if (checksum != logBlock.checksum)
            throw new Exception("checksum error for logBlock=" + logBlock);
//        System.out.println("read block " + logBlock);
        return logBlock;
    }

    private static byte[] getByteArray(FileChannel channel, ByteBuffer buffer) throws Exception {
        int length = readNextArray(channel, buffer);
        return getByteArrayFromBuffer(buffer, length);
    }

    private static byte[] getByteArray(FileChannel channel,
                                       ByteBuffer buffer, int length) throws Exception {
        readNextArrayWithN(channel, buffer, length);
        return getByteArrayFromBuffer(buffer, length);
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

    private static byte[] getByteArrayFromBuffer(ByteBuffer buffer, int len) throws Exception {
        if (buffer.limit() == 0) throw new Exception("ByteBuffer empty");
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public String toString() {
        return "Logs.LogBlock{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", time=" + time +
                ", operations=" + operations +
                ", checksum=" + checksum +
                '}';
    }
}
