package sstIo;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

public class MMappedReader extends ChannelBackedReader{
    private MappedByteBuffer map;
    public MMappedReader(File file) throws IOException {
        //System.out.println("Constructor for file="+file.getName());
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "r");
        this.channel = randomAccessFile.getChannel();

        this.map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
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
    public long position() throws IOException {
        return map.position();
    }

    @Override
    public void position(int positionToMove) {
        map.position(positionToMove);
    }

    @Override
    public void close() throws IOException {
        if (map != null) {
            unmap(map);
        }
        map = null;
        channel.close();
        randomAccessFile.close();
    }
}