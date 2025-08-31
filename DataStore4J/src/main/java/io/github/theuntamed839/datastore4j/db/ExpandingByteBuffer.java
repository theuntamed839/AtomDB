package io.github.theuntamed839.datastore4j.db;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ExpandingByteBuffer extends OutputStream {
    private static final int DEFAULT_CAPACITY = 1024;

    private ByteBuffer buffer;

    public ExpandingByteBuffer() {
        this.buffer = ByteBuffer.allocateDirect(DEFAULT_CAPACITY);
    }

    public ExpandingByteBuffer put(byte[] data) {
        makeSpaceIfNeeded(data.length);
        buffer.put(data);
        return this;
    }

    public ExpandingByteBuffer put(byte data) {
        makeSpaceIfNeeded(1);
        buffer.put(data);
        return this;
    }

    public ExpandingByteBuffer putInt(int data) {
        makeSpaceIfNeeded(Integer.BYTES);
        buffer.putInt(data);
        return this;
    }

    public ExpandingByteBuffer putLong(long data) {
        makeSpaceIfNeeded(Long.BYTES);
        buffer.putLong(data);
        return this;
    }

    private void makeSpaceIfNeeded(int requiredSize) {
        if (buffer.remaining() < requiredSize) {
            int newCapacity = Math.max(buffer.capacity() * 2, buffer.position() + requiredSize);
            ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);

            buffer.flip();
            newBuffer.put(buffer);

            buffer = newBuffer;
        }
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void clear() {
        buffer.clear();
    }

    public ExpandingByteBuffer flip() {
        buffer.flip();
        return this;
    }

    public ExpandingByteBuffer put(byte[] src, int offset, int length) {
        makeSpaceIfNeeded(length);
        buffer.put(src, offset, length);
        return this;
    }

    public int position() {
        return this.buffer.position();
    }

    public ExpandingByteBuffer position(int position) {
        this.buffer.position(position);
        return this;
    }

    public long remaining() {
        return this.buffer.remaining();
    }

    @Override
    public void write(int b) throws IOException {
        makeSpaceIfNeeded(Byte.BYTES);
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        makeSpaceIfNeeded(b.length);
        buffer.put(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        makeSpaceIfNeeded(len);
        buffer.put(b, off, len);
    }

    public byte get() {
        return buffer.get();
    }

    public void limit(int size) {
        buffer.limit(size);
    }

    public void get(byte[] item) {
        buffer.get(item);
    }

    public long getLong() {
        return buffer.getLong();
    }

    public int getInt() {
        return buffer.getInt();
    }

    public void get(byte[] item, int offset, int length) {
        buffer.get(item, offset, length);
    }
}
