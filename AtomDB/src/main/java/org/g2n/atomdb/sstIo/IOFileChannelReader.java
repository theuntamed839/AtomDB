package org.g2n.atomdb.sstIo;

import org.g2n.atomdb.db.ExpandingByteBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * TODO:
 * 1. Can try, reading the whole file into Bytebuffer as our ssts are small enough
 * 2. or Page Size reading in buffer.
 */
public class IOFileChannelReader extends IOReader {
    private java.nio.channels.FileChannel channel;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public IOFileChannelReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
    }

    @Override
    public int read() throws IOException {
        return extractValue(Byte.BYTES).get();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return getBytes(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return getBytes(b, off, len);
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
    public byte getByte() throws IOException {
        return extractValue(Byte.BYTES).get();
    }

    @Override
    public int getBytes(byte[] item) throws IOException {
        var buffer = extractValue(item.length);
        buffer.get(item);
        return buffer.position();
    }

    @Override
    public int getBytes(byte[] item, int offset, int length) throws IOException {
        channel.position(offset);
        var buffer = extractValue(length);
        buffer.get(item);
        return buffer.position();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public void position(int positionToMove) throws IOException {
        channel.position(positionToMove);
    }

    @Override
    public boolean stillAvailable() throws IOException {
        return channel.isOpen() && channel.position() < channel.size();
    }

    @Override
    public byte get() throws IOException {
        return extractValue(Byte.BYTES).get();
    }

    @Override
    public void get(byte[] item) throws IOException {
        var buffer = extractValue(item.length);
        buffer.get(item);
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
