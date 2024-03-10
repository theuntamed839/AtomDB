package sstIo;

import util.SizeOf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

// important always keep the buffer size small then pagesize
public class SSTWriteWithBuffer extends Writer{
    public static final int PAGE_SIZE = 4 * SizeOf.MB;
    private final FileChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(4 * SizeOf.MB);
    private MappedByteBuffer map;
    private int mapOffset = 0;
    public SSTWriteWithBuffer(FileChannel channel) throws IOException {
        this.channel = channel;
        map = channel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    public SSTWriter putLong(long item) {
        writeContentIfBufferFull(SizeOf.LongLength);
        buffer.putLong(item);
        return this;
    }

    public SSTWriter putInt(int item) {
        writeContentIfBufferFull(SizeOf.IntLength);
        buffer.putInt(item);
        return this;
    }

    public SSTWriter putByte(byte item) {
        writeContentIfBufferFull(Byte.BYTES);
        buffer.put(item);
        return this;
    }

    public SSTWriter putBytes(byte[] item) {
        writeContentIfBufferFull(Byte.BYTES * item.length);
        buffer.put(item);
        return this;
    }

    @Override
    public long position() throws IOException {
        return map.position() + buffer.position();
    }

    @Override
    public void writeAtPositionInIsolation(long position, long binarySearchLocation) throws IOException {
        long previousPosition = channel.position();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8);
        byteBuffer.putLong(binarySearchLocation);
        byteBuffer.flip();
        channel.write(byteBuffer, position);
        // todo do i need to move it back to the place ??
        channel.position(previousPosition);
    }

    @Override
    public void write(int b) throws IOException {
        putInt(b);
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
                map = channel.map(FileChannel.MapMode.READ_WRITE, mapOffset, PAGE_SIZE);
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
        channel.close();
    }
}
