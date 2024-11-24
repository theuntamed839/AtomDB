package db;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ExpandingByteBuffer {
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
        makeSpaceIfNeeded(4);
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
        // Returns the buffer in its current state
        return buffer;
    }

    public byte[] toByteArray() {
        // Prepare buffer for reading and convert to byte array
        buffer.flip();
        byte[] byteArray = new byte[buffer.remaining()];
        buffer.get(byteArray);
        return byteArray;
    }

    public void clear() {
        // Reset the buffer
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
}
