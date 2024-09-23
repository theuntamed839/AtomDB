package sstIo;

import java.io.IOException;

public interface PrimitiveReader extends AutoCloseable{
    long getLong();
    int getInt();
    byte getByte();
    int getBytes(byte[] item);
    int getBytes(byte[] item, int offset, int length);
    long position() throws IOException;
    void position(int positionToMove);
}
