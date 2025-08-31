package io.github.theuntamed839.datastore4j.sstIO;

import io.github.theuntamed839.datastore4j.db.ExpandingByteBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * TODO:
 * 1. Page Size reading in buffer.
 */
public class IOFileChannelReader extends IOReader {
    private java.nio.channels.FileChannel channel;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public IOFileChannelReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
    }

    @Override
    public int read() throws IOException {
        return hasRemaining() ? (extractValue(Byte.BYTES).get() & 0xFF) : -1;
    }

    @Override
    public int read(byte[] item) throws IOException {
        return read(item, 0, item.length);
    }

    @Override
    public int read(byte[] item, int offset, int length) throws IOException {
        if (!hasRemaining()) {
            return -1;
        }
        if (item == null) {
            throw new NullPointerException();
        }
        if (offset < 0 || length < 0 || offset + length > item.length) {
            throw new IndexOutOfBoundsException();
        }

        int numberOfBytesCouldBeRead = (int) Math.min(length, remaining());
        extractValue(numberOfBytesCouldBeRead).get(item, offset, numberOfBytesCouldBeRead);
        return numberOfBytesCouldBeRead;
    }

    @Override
    public long getLong() throws IOException {
        return extractValue(Long.BYTES).getLong();
    }

    @Override
    public int getInt() throws IOException {
        return extractValue(Integer.BYTES).getInt();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public void position(long positionToMove) throws IOException {
        channel.position(positionToMove);
    }

    @Override
    public boolean hasRemaining() throws IOException {
        return channel.isOpen() && channel.position() < channel.size();
    }

    private long remaining() throws IOException {
        return channel.size() - channel.position();
    }

    @Override
    public byte get() throws IOException {
        return extractValue(Byte.BYTES).get();
    }

    @Override
    public void get(byte[] item) throws IOException {
        read(item, 0, item.length);
    }

    private ExpandingByteBuffer extractValue(int length) throws IOException {
        var buffer = bufferThreadLocal.get();
        buffer.clear();
        buffer.limit(length);
        channel.read(buffer.getBuffer());
        buffer.flip();
        return buffer;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        channel = null;
    }
}
