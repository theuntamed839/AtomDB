package org.g2n.atomdb.SSTIO;

import java.io.IOException;

public interface PrimitiveWriter extends AutoCloseable {
    PrimitiveWriter putLong(long item);
    PrimitiveWriter putInt(int item);
    PrimitiveWriter putByte(byte item);
    PrimitiveWriter putBytes(byte[] item);
    PrimitiveWriter putBytes(byte[] item, int offset, int length);
    long position() throws IOException;
    void position(long positionToMove);
}


/**
 * Todo
 * 1. direct mappedByteBuffer which directly writes based on remaining.
 * 2. buffer using the bytebuffer and writes in chunk where our buffer is fixed 4mb.
 * 3.
 */