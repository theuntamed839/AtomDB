package sstIo;

import Constants.DBConstant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

/**
 * todo
 *  1. can we divide this class, where the buffer is abstracted out in some other class, since this is kinda cache or something which adds up to the responsibility
 *  2. if we do this then we can have a mmapped and a channel based classes and both can be wrapped in buffered class.
 */
public class FullFileBufferedWriter extends ChannelBackedWriter {
    private ByteBuffer buffer = ByteBuffer.allocateDirect(DBConstant.WRITER_BUFFER_SIZE);
    public FullFileBufferedWriter(File file) throws IOException {
        this.file = file;
    }

    public PrimitiveWriter putLong(long item) {
        writeContentIfBufferFull(Long.BYTES);
        buffer.putLong(item);
        return this;
    }

    public PrimitiveWriter putInt(int item) {
        writeContentIfBufferFull(Integer.BYTES);
        buffer.putInt(item);
        return this;
    }

    public PrimitiveWriter putByte(byte item) {
        writeContentIfBufferFull(Byte.BYTES);
        buffer.put(item);
        return this;
    }

    public PrimitiveWriter putBytes(byte[] item) {
        writeContentIfBufferFull(Byte.BYTES * item.length);
        buffer.put(item);
        return this;
    }

    @Override
    public PrimitiveWriter putBytes(byte[] item, int offset, int length) {
        writeContentIfBufferFull(Byte.BYTES * (length - offset));
        buffer.put(item, offset, length);
        return this;
    }

    // outputStream
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

    private void writeContentIfBufferFull(int requiredSpace) {
        if (buffer.remaining() < requiredSpace) {
            System.out.println("I CURRENT SUITATION THIS SHOULDNOT HIT".repeat(100));
            var newBuff = ByteBuffer.allocate((buffer.remaining() + buffer.position()) * 2);
            buffer.flip();
            newBuff.put(buffer);
            buffer = newBuff;
        }
    }

    @Override
    public void close() throws IOException {
        buffer.flip();
        var randomAccessFile = new RandomAccessFile(file, "rw");
        var channel = randomAccessFile.getChannel();
        var map = channel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.remaining());
        map.put(buffer);
        buffer.clear();
        unmap(map);
    }
}
