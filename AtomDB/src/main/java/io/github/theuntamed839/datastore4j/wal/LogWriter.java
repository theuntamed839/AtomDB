package io.github.theuntamed839.datastore4j.wal;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface LogWriter extends AutoCloseable {
    void write(byte[] arr) throws IOException;
    void write(ByteBuffer buffer) throws IOException;
}
