package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class IOMMappedReader extends IOReader {
    private final Arena arena;
    private final ByteBuffer map;
    private final FileChannel channel;

    public IOMMappedReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        this.arena = Arena.ofShared();
        this.map = this.channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena).asByteBuffer();
    }

    @Override
    public int read() throws IOException {
        return map.hasRemaining() ? (map.get() & 0xFF) : -1;
    }

    @Override
    public int read(byte[] item) throws IOException {
        return read(item, 0, item.length);
    }

    @Override
    public int read(byte[] item, int offset, int length) throws IOException {
        if (!map.hasRemaining()) {
            return -1;
        }
        if (item == null) {
            throw new NullPointerException();
        }
        if (offset < 0 || length < 0 || offset + length > item.length) {
            throw new IndexOutOfBoundsException();
        }

        int numberOfBytesCouldBeRead = Math.min(length, map.remaining());
        map.get(item, offset, numberOfBytesCouldBeRead);
        return numberOfBytesCouldBeRead;
    }

    @Override
    public long getLong() {
        return map.getLong();
    }

    @Override
    public int getInt() {
        return map.getInt();
    }

    @Override
    public long position() {
        return map.position();
    }

    @Override
    public void position(long positionToMove) {
        map.position((int) positionToMove);
    }

    @Override
    public boolean hasRemaining() {
        return map.hasRemaining();
    }

    @Override
    public byte get() {
        return map.get();
    }

    @Override
    public void get(byte[] item) throws IOException {
        read(item, 0, item.length);
    }

    @Override
    public void close() throws IOException {
        arena.close();
        channel.close();
    }
}