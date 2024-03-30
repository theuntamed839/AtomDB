package sstIo;

import Constants.DBConstant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

/**
 * todo
 *  1. can we divide this class, where the buffer is abstracted out in some other class, since this is kinda cache or something which adds up to the responsibility
 *  2. if we do this then we can have a mmapped and a channel based classes and both can be wrapped in buffered class.
 */
public class BufferedMMappedWriter extends ChannelBackedWriter {
    public static final int PAGE_SIZE = DBConstant.PAGE_SIZE;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);
    private MappedByteBuffer map;
    private int mapOffset = 0;
    public BufferedMMappedWriter(File file) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.channel = randomAccessFile.getChannel();
        this.map = channel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
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

    @Override
    public void write(int b) throws IOException {
        putInt(b);
    }

    @Override
    public long position() throws IOException {
        return mapOffset + map.position() + buffer.position();
    }

    @Override
    public void position(long positionToMove) {
        writeContents();
        remapToPosition(positionToMove);
    }

    private void remapToPosition(long positionToMove) {
        unmap(map);
        try {
            mapOffset = (int) positionToMove;
            map = channel.map(FileChannel.MapMode.READ_WRITE, mapOffset, PAGE_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeContentIfBufferFull(int requiredSpace) {
        if (buffer.remaining() < requiredSpace) {
            writeContents();
        }
    }

    private void writeContents() {
        buffer.flip();
        ensureCapacity(buffer.remaining());
        map.put(buffer);
        buffer.clear();
    }

    private void ensureCapacity(int bytes) {
        if (map.remaining() < bytes) {
            // remap
            mapOffset += map.position();
            unmap(map);
            try {
                int requiredSize = Math.max(bytes, PAGE_SIZE);
                map = channel.map(FileChannel.MapMode.READ_WRITE, mapOffset, requiredSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (buffer.hasRemaining()) {
            writeContents();
        }
        unmap(map);
    }
}
