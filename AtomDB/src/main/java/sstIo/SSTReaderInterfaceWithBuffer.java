package sstIo;

import util.SizeOf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

public class SSTReaderInterfaceWithBuffer extends ReaderInterface {

    public static final int PAGE_SIZE = 1 * SizeOf.MB;
    private final FileChannel channel;
    //private final ByteBuffer buffer = ByteBuffer.allocateDirect(4 * SizeOf.MB);
    private MappedByteBuffer map;
    private int mapOffset = -1;
    public SSTReaderInterfaceWithBuffer(FileChannel channel) throws IOException {
        this.channel = channel;
        /**
         * todo currently accessing full file we can do partial for big files
         *
         */
       // map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }

    @Override
    public int read() throws IOException {
        return map.getInt();
    }

    @Override
    public void close() throws IOException {
        if (map != null) {
            unmap(map);
        }
        channel.close();
    }

    public ByteBuffer readSize(byte[] arr, long position, int length){
//        System.out.println("readSize with position="+position + " lenght="+ length);
//        System.out.println(map == null ? "null" : "mapOffset="+mapOffset + " map.pos="+map.position());
        ensureCapacity(position, length);
//        System.out.println("before mmap position change ="+ map.position());
        map.position((int) (position - mapOffset));
        //System.out.println("after mmap position change ="+ map.position());
        return readSize(arr, length);
    }

    public ByteBuffer readSize(byte[] arr, int length){
        //System.out.println("readSize without position");
        ensureCapacity(mapOffset + map.position(), length);
        map.get(arr, 0, length);
        return ByteBuffer.wrap(arr);
    }

    private void ensureCapacity(long position, int length){
        if (mapOffset != -1 && mapOffset <= position && mapOffset + PAGE_SIZE >= position + length) {
            return;
        }
        var SIZE = PAGE_SIZE;
        if (PAGE_SIZE < length) {
            SIZE = length;
            //throw new RuntimeException("pagesize=" + PAGE_SIZE + "trying to retrive length="+ length + "We are not prepared for this, trying to access data which is beyond the page size limit we need to improve the system");
        }
        mapOffset = (int) position;
        try {
            if (map != null) {
                unmap(map);
            }
            map = channel.map(FileChannel.MapMode.READ_ONLY, mapOffset, SIZE);
//            System.out.println("mapped file from the position=" + mapOffset);
//            System.out.println("map position="+map.position());
        } catch (IOException e) {
            //System.out.println("SIZW="+SIZE);
            throw new RuntimeException(e);
        }
    }
}
