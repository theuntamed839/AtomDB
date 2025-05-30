package org.g2n.atomdb.sstIo;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MMappedBackedReader extends IOReader {
    private final Arena arena;
    private final ByteBuffer map;
    private final FileChannel channel;

    public MMappedBackedReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        this.arena = Arena.ofShared(); // TODO: can we have this ofConfined ?
        this.map = this.channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena).asByteBuffer();
    }

    // input stream
    @Override
    public int read() throws IOException {
        return map.get();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return getBytes(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return getBytes(b, off, len);
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
    public byte getByte() {
        return map.get();
    }

    @Override
    public int getBytes(byte[] item) {
        map.get(item);
        return item.length;
    }

    @Override
    public int getBytes(byte[] item, int offset, int length) {
        map.get(item, offset, length);
        return item.length;
    }

    @Override
    public long position() {
        return map.position();
    }

    @Override
    public void position(int positionToMove) {
        map.position(positionToMove);
    }

    @Override
    public void close() throws IOException {
        arena.close();
        channel.close();
    }

    @Override
    public boolean stillAvailable() {
        return map.hasRemaining();
    }

    @Override
    public byte get() {
        return map.get();
    }

    @Override
    public void get(byte[] k) {
        map.get(k);
    }
}