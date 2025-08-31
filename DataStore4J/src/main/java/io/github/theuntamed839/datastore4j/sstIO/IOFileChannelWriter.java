package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class IOFileChannelWriter extends IOWriter{
    private final FileChannel fileChannel;
    private final long fileSize;
    private long writtenBytes = 0;
    private boolean isClosed = false;
    public IOFileChannelWriter(Path path, long fileSize) throws IOException {
        if (fileSize <= 0) {
            throw new java.lang.IllegalArgumentException("Requested bytes size must be >= 0.");
        }
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.fileSize = fileSize;
    }

    @Override
    public void put(ByteBuffer buffer) throws IOException {
        if (writtenBytes + buffer.remaining() > fileSize) {
            throw new BufferOverflowException();
        }

        while (buffer.hasRemaining()) {
            writtenBytes += fileChannel.write(buffer);
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed) {
            throw new IllegalStateException("Writer is already closed.");
        }
        this.fileChannel.close();
        isClosed = true;
    }
}
