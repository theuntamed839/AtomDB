package Logs;

import Checksum.Checksum;
import Checksum.Crc32cChecksum;
import Constants.Operations;
import util.SizeOf;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class LogBlock {
    private final byte[] key;
    private final byte[] value;
    private final Operations operations;
    private final long checksum;
    private final static byte[] temp=new byte[1];

    public LogBlock(Operations operations, byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        this.operations = operations;
        Checksum checksumProvide = new Crc32cChecksum();
        this.checksum = checksumProvide.logBlock(operations.value(), key, value != null ? value : temp);
    }

    // Writing section
    public ByteBuffer getBytes() {
        // todo performance ?
        var buffer = ByteBuffer.allocate(getTotalBytesRequiredForLogBlock());
        buffer.putLong(getLogBlockSize())
                .put(operations.value())
                .putInt(key.length)
                .put(key);
        if (operations != Operations.DELETE) {
            buffer.putInt(value.length)
                    .put(value);
        }
        buffer.putLong(checksum);
        buffer.flip();
        return buffer;
    }

    public int getTotalBytesRequiredForLogBlock() {
       return  SizeOf.LongLength +
               SizeOf.OperationsLength +
               SizeOf.IntLength + // key length
               key.length +
               (operations == Operations.WRITE ? (SizeOf.IntLength + value.length ) : 0) + // value length and value
               SizeOf.LongLength; // checksum
    }

    private int getLogBlockSize() {
        return getTotalBytesRequiredForLogBlock() - SizeOf.LongLength; // since we won't be reading the first long again.
    }

    // reader
    public static LogBlock read(Reader reader) {
        ByteBuffer buffer = getRequiredBytes(reader);

        // operation read
        Operations op = Operations.getOP(buffer.get());

        // key read
        byte[] key = getByteArray(buffer);

        byte[] value = null;
        if (op != Operations.DELETE) {
            //value read
            value = getByteArray(buffer);
        }

        // checksum read
        long checksum = buffer.getLong();
        LogBlock logBlock = new LogBlock(op, key, value);

        if (checksum != logBlock.checksum) {
            throw new RuntimeException("Checksum mismatch"+logBlock);
        }
        return logBlock;
    }

    private static ByteBuffer getRequiredBytes(Reader reader) {
        ByteBuffer buffer = reader.readFromCurrentPosition(SizeOf.LongLength);
        long totalSize = buffer.getLong();
        buffer = reader.readFromCurrentPosition((int) totalSize);
        return buffer;
    }

    private static byte[] getByteArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        var arr = new byte[length];
        buffer.get(arr);
        return arr;
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

    public Operations getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "Logs.LogBlock{" +
                "key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", operations=" + operations +
                ", checksum=" + checksum +
                '}';
    }

}
