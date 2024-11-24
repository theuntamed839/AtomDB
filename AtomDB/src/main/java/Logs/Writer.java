package Logs;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Writer extends AutoCloseable {
    void write(byte[] arr) throws IOException;
    void write(ByteBuffer buffer) throws IOException;
}
