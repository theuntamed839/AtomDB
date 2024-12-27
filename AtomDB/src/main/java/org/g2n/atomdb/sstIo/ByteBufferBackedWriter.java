package org.g2n.atomdb.sstIo;

import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.db.ExpandingByteBuffer;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class ByteBufferBackedWriter extends ChannelBackedWriter {
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(DBConstant.WRITER_BUFFER_SIZE);

    public ByteBufferBackedWriter(File file) {
        this.fileToWrite = file;
    }

    public PrimitiveWriter putLong(long item) {
        buffer.putLong(item);
        return this;
    }

    public PrimitiveWriter putInt(int item) {
        buffer.putInt(item);
        return this;
    }

    public PrimitiveWriter putByte(byte item) {
        buffer.put(item);
        return this;
    }

    public PrimitiveWriter putBytes(byte[] item) {
        buffer.put(item);
        return this;
    }

    @Override
    public PrimitiveWriter putBytes(byte[] item, int offset, int length) {
        buffer.put(item, offset, length);
        return this;
    }

    @Override
    public void write(int b) throws IOException {
        putByte((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        putBytes(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        putBytes(b, off, len);
    }

    @Override
    public long position() throws IOException {
        return buffer.position();
    }

    @Override
    public void position(long positionToMove) {
        buffer.position((int) positionToMove);
    }

    @Override
    public void close() throws IOException {
        buffer.flip();
        try (
                var fileChannel = FileChannel.open(fileToWrite.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
                var arena = Arena.ofConfined())
        {
            var fileSegment = fileChannel.map(READ_WRITE, 0, buffer.remaining(), arena);
            fileSegment.asByteBuffer().put(buffer);
        } finally {
            buffer.clear();
        }
    }
}
