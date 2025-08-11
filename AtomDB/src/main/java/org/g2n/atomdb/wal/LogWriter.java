package org.g2n.atomdb.wal;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface LogWriter extends AutoCloseable {
    void write(byte[] arr) throws IOException;
    void write(ByteBuffer buffer) throws IOException;
}
