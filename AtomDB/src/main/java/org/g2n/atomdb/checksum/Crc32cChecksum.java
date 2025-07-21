package org.g2n.atomdb.checksum;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;


public class Crc32cChecksum implements AtomChecksum {
    private static final ThreadLocal<Checksum> crc32cThreadLocal = ThreadLocal.withInitial(CRC32C::new);
    private static final int BUFFER_CAPACITY = 1024;

    private static final ThreadLocal<ByteBuffer> byteBufferThreadLocal = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(BUFFER_CAPACITY)
    );
    static Crc32cChecksum instance;
    static {
        instance = new Crc32cChecksum();
    }

    public static Crc32cChecksum getInstance() {
        return instance;
    }


    @Override
    public long compute(byte[] arr) {
        var crc32c = crc32cThreadLocal.get();
        crc32c.reset();
        ByteBuffer buffer = prepareBuffer(arr);
        crc32c.update(buffer);
        return crc32c.getValue();
    }

    @Override
    public long compute(byte[] key, byte[] value) {
        var crc32c = crc32cThreadLocal.get();
        crc32c.reset();
        crc32c.update(prepareBuffer(key));
        crc32c.update(prepareBuffer(value));
        return crc32c.getValue();
    }

    private ByteBuffer prepareBuffer(byte[] arr) {
        ByteBuffer buffer = byteBufferThreadLocal.get();
        buffer.clear();
        if (arr.length > buffer.capacity()) {
            buffer = ByteBuffer.allocateDirect(arr.length);
            byteBufferThreadLocal.set(buffer);
        }
        buffer.put(arr);
        buffer.flip();
        return buffer;
    }
}
