package sstIo;

import java.io.IOException;

public interface SSTWriter extends AutoCloseable {
    SSTWriter putLong(long item);
    SSTWriter putInt(int item);
    SSTWriter putByte(byte item);
    SSTWriter putBytes(byte[] item);
    long position() throws IOException;
    void writeAtPositionInIsolation(long position, long value) throws IOException;
}


/**
 * Todo
 * 1. direct mappedByteBuffer which directly writes based on remaining.
 * 2. buffer using the bytebuffer and writes in chunk where our buffer is fixed 4mb.
 * 3.
 */