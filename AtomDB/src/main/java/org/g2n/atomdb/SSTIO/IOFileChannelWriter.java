package org.g2n.atomdb.SSTIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class IOFileChannelWriter extends IOWriter{
    private final FileChannel fileChannel;

    public IOFileChannelWriter(Path path, long fileSize) throws IOException {
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
//        this.fileChannel.truncate(fileSize);
    }

    @Override
    public void put(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    @Override
    public void close() throws Exception {
        this.fileChannel.close();
    }
}
