package sstIo;

import Constants.DBConstant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

public class BufferedMMappedReader extends ChannelBackedReader{
    public static final int PAGE_SIZE = DBConstant.PAGE_SIZE;
    private MappedByteBuffer map;
    private int mapOffset = 0;
    public BufferedMMappedReader(File file) throws IOException {
        System.out.println("creating reader for="+file);
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.channel = randomAccessFile.getChannel();
        this.map = channel.map(FileChannel.MapMode.READ_WRITE, mapOffset, channel.size());
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
        ensureCapacity(Long.BYTES);
        return map.getLong();
    }

    @Override
    public int getInt() {
        ensureCapacity(Integer.BYTES);
        return map.getInt();
    }

    @Override
    public byte getByte() {
        ensureCapacity(Byte.BYTES);
        return map.get();
    }

    @Override
    public int getBytes(byte[] item) {
        ensureCapacity(item.length);
        map.get(item);
        return item.length;
    }

    @Override
    public int getBytes(byte[] item, int offset, int length) {
        ensureCapacity(Math.abs(item.length - offset));
        map.get(item, offset, length);
        return item.length;
    }

    @Override
    public long position() throws IOException {
        return mapOffset + map.position();
    }


    @Override
    public void position(int positionToMove) {
        if (mapOffset < positionToMove && mapOffset + map.limit() > positionToMove) {
            // in the mapped region.
            map.position(positionToMove - mapOffset);
        }
        else {
            map(positionToMove);
        }
    }

    private void map(int position) {
        try {
            mapOffset = position;
            map = channel.map(FileChannel.MapMode.READ_WRITE, mapOffset, PAGE_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureCapacity(int bytes) {
        if (map.remaining() < bytes) {
            // remap
            mapOffset += map.position();
            unmap(map);
            try {
                int requiredSize = Math.max(bytes, PAGE_SIZE);
                map = channel.map(FileChannel.MapMode.READ_WRITE, mapOffset, requiredSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        unmap(map);
    }
}