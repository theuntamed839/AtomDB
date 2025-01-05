package org.g2n.atomdb.sstIo;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class MMappedReader extends InputStream {
    private final Arena arena;
    private final ByteBuffer map;
    private final FileChannel channel;

    public MMappedReader(File file) throws IOException {
        //System.out.println("Constructor for fileToWrite="+fileToWrite.getName());
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.arena = Arena.ofShared();
        this.map = this.channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena).asByteBuffer();
    }

    // input stream
    @Override
    public int read() throws IOException {
        return getByte();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return getBytes(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return getBytes(b, off, len);
    }

    public long getLong() {
        return map.getLong();
    }

    public int getInt() {
        return map.getInt();
    }

    public byte getByte() {
        return map.get();
    }

    public int getBytes(byte[] item) {
        map.get(item);
        return item.length;
    }

    public int getBytes(byte[] item, int offset, int length) {
        map.get(item, offset, length);
        return item.length;
    }

    public long position() {
        return map.position();
    }

    public void position(int positionToMove) {
        map.position(positionToMove);
    }

    @Override
    public void close() throws IOException {
        arena.close();
        channel.close();
    }
}