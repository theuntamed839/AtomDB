package org.g2n.atomdb.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public final class SynchronizedFileChannelWriter implements Writer, SynchronizedWriter {
    private final FileChannel channel;

    public SynchronizedFileChannelWriter(Path filePath) throws IOException {
        this.channel = FileChannel.open(filePath, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE);
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public synchronized void write(byte[] buffer) throws IOException {
        write(ByteBuffer.wrap(buffer));
    }

    @Override
    public synchronized void write(ByteBuffer buffer) throws IOException {
        channel.write(buffer);
    }
}
