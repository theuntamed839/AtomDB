package org.g2n.atomdb.sstIo;

import org.g2n.atomdb.db.ExpandingByteBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileChannelBackedReader extends IOReader {
    private final java.nio.channels.FileChannel channel;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public FileChannelBackedReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
    }

    @Override
    public int read() throws IOException {
        if (channel.position() >= channel.size()) {
            return -1;
        }
        var buffer = bufferThreadLocal.get();
        buffer.clear();
        channel.read(buffer.getBuffer());
        buffer.flip();
        return buffer.get();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return 0;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return 0;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public int getInt() {
        return 0;
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public int getBytes(byte[] item) {
        return 0;
    }

    @Override
    public int getBytes(byte[] item, int offset, int length) {
        return 0;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void position(int positionToMove) {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean stillAvailable() {
        return false;
    }

    @Override
    public byte get() {
        return 0;
    }

    @Override
    public void get(byte[] k) {

    }
}
