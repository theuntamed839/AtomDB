package db;

import java.nio.ByteBuffer;

public class GlobalByteBuffer {
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE)
    );

    public static ByteBuffer getBuffer() {
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        return buffer;
    }
}
