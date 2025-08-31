package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class IOMMappedWriter extends IOWriter{
    private final FileChannel fileChannel;
    private final Arena arena;
    private final ByteBuffer buffer;

    public IOMMappedWriter(Path path, long fileSize) throws IOException {
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.arena = Arena.ofConfined();
        this.buffer = fileChannel.map(READ_WRITE, 0, fileSize, arena).asByteBuffer();
    }

    @Override
    public void put(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            this.buffer.put(buffer);
        }
    }

    @Override
    public void close() throws Exception {
        this.arena.close();
        this.fileChannel.close();
    }
}
