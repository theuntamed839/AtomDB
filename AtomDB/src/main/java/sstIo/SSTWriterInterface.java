package sstIo;

import java.io.IOException;

public interface SSTWriterInterface extends AutoCloseable {
    SSTWriterInterface putLong(long item);
    SSTWriterInterface putInt(int item);
    SSTWriterInterface putByte(byte item);
    SSTWriterInterface putBytes(byte[] item);
    long position() throws IOException;
    void writeAtPositionInIsolation(long position, long value) throws IOException;
}


/**
 * Todo
 * 1. direct mappedByteBuffer which directly writes based on remaining.
 * 2. buffer using the bytebuffer and writes in chunk where our buffer is fixed 4mb.
 * 3.
 */