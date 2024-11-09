package Logs;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Reader extends AutoCloseable{
    byte[] readFrom(byte[] arr, long position, int length);
    byte[] readFromCurrentPosition(byte[] arr, int length);
    ByteBuffer readFrom(long position, int length);
    ByteBuffer readFromCurrentPosition(int length);
    long fileSize() throws IOException;
    void setPosition(long position);
    int getPosition();
}
