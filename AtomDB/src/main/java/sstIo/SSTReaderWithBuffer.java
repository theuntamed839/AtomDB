package sstIo;

import util.SizeOf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

public class SSTReaderWithBuffer extends Reader{

    public static final int PAGE_SIZE = 4 * SizeOf.MB;
    private final FileChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(4 * SizeOf.MB);
    private MappedByteBuffer map;
    private int mapOffset = 0;
    public SSTReaderWithBuffer(FileChannel channel) throws IOException {
        this.channel = channel;
        /**
         * todo currently accessing full file we can do partial for big files
         *
         */
        map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }

    @Override
    public int read() throws IOException {
        return map.getInt();
    }

    @Override
    public void close() throws IOException {
        unmap(map);
        channel.close();
    }

    public ByteBuffer readSize(byte[] arr, long position, int length) {
        map.position((int) position);
        return readSize(arr, length);
    }

    public ByteBuffer readSize(byte[] arr, int length) {
        map.get(arr, 0, length);
        return ByteBuffer.wrap(arr);
    }
}
