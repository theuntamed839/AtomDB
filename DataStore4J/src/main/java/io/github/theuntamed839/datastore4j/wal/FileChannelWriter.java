package io.github.theuntamed839.datastore4j.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public final class FileChannelWriter implements LogWriter {
    private final FileChannel channel;

    public FileChannelWriter(Path filePath) throws IOException {
        this.channel = FileChannel.open(filePath, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE);
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public void write(byte[] buffer) throws IOException {
        write(ByteBuffer.wrap(buffer));
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException {
        channel.write(buffer);
    }
}
