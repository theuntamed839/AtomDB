package Logs;

import Constants.DBConstant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static util.ByteBufferSupport.unmap;

/**
 * replace duplicate with asReadOnlyBuffer
 */
public class MMapFileReaderPartialMapActAsChannel implements Reader {
    public static final int PAGE_SIZE = DBConstant.PAGE_SIZE;
    private final FileChannel channel;
    private final RandomAccessFile randomAccessFile;
    private MappedByteBuffer map;
    private int mapOffset = -1;
    private final long channelSize;
    public MMapFileReaderPartialMapActAsChannel(File file) throws IOException {
        randomAccessFile = new RandomAccessFile(file, "r");
        channel = randomAccessFile.getChannel();
        channelSize = channel.size();
    }

    public long fileSize() throws IOException {
        return channel.size();
    }

    @Override
    public void setPosition(long position) {
        ensureCapacity(position, 0);
        map.position((int) (position - mapOffset));
    }

    @Override
    public byte[] readFrom(byte[] arr, long position, int length) {
        ensureCapacity(position, length);
        map.position((int) (position - mapOffset));
        map.get(arr, 0, length);
        return arr;
    }

    @Override
    public byte[] readFromCurrentPosition(byte[] arr, int length) {
        ensureCapacity(mapOffset + map.position(), length);
        map.get(arr, 0, length);
        return arr;
    }

    @Override
    public ByteBuffer readFrom(long position, int length) {
        ensureCapacity(position, length);
        map.position((int) (position - mapOffset));
        return map.duplicate().clear().limit((int) (position + length)).position(map.position());
    }

    @Override
    public ByteBuffer readFromCurrentPosition(int length) {
        ensureCapacity(mapOffset + map.position(), length);
        var buff =  map.duplicate().clear().limit(map.position() + length).position(map.position());
        map.position(map.position() + length);
        return buff;
    }

    // todo as per reading goes we should use slice instead of duplicate. but this will create another object
    //  we need to benchmark the performance hit, if less then consider the one which is more readable.
//    @Override
//    public ByteBuffer readFrom(long position, int length) {
//        ensureCapacity(position, length);
//        map.position((int) (position - mapOffset));
//        ByteBuffer duplicateBuffer = map.duplicate();
//        duplicateBuffer.limit(map.position() + length);
//        return duplicateBuffer.slice(); // Use slice instead of clear to maintain position
//    }
//
//    @Override
//    public ByteBuffer readFromCurrentPosition(int length) {
//        ensureCapacity(mapOffset + map.position(), length);
//        ByteBuffer duplicateBuffer = map.duplicate();
//        int currentPosition = duplicateBuffer.position();
//        duplicateBuffer.limit(currentPosition + length);
//        map.position(currentPosition + length); // Update position of original buffer
//        return duplicateBuffer.slice(); // Use slice instead of clear to maintain position
//    }

    private void ensureCapacity(long position, int length) {
        // Check if the requested position and length are within the currently mapped region
        if (isWithinMappedRegion(position, length)) {
            return;
        }

        // Check if the requested position and length exceed the channel size
        if (position + length > channelSize) {
            throw new IllegalArgumentException("Channel size is too small for the required length. Channel Size: " + channelSize + ", Required Length: " + length);
        }

        // Determine the minimum viable size for mapping based on the remaining space in the channel
        int minViableSize = calculateMinViableSize();

        // Adjust the required size to ensure it covers the requested length
        int requiredSize = Math.max(minViableSize, length);

        // Map the required region into memory
        try {
            mapRegion(position, requiredSize);
        } catch (IOException e) {
            throw new RuntimeException("requiredSize="+requiredSize+" channelSize="+channelSize +
                    "request from offset="+position + " request length="+length+
                    "map="+map+" current channelOffset="+mapOffset, e);
//            throw new RuntimeException("Failed to map region into memory.", e);
        }
    }

    private boolean isWithinMappedRegion(long position, int length) {
        return mapOffset != -1 &&
                mapOffset <= position &&
                mapOffset + map.limit() >= position + length;
    }

    private int calculateMinViableSize() {
        int remainingSpace = (int) ((map != null) ? channelSize - (mapOffset + map.position()) : channelSize);
        return Math.min(PAGE_SIZE, remainingSpace);
    }

    private void mapRegion(long position, int requiredSize) throws IOException {
        // todo, we may not unmap it right away as this thing can be done by GC.
        //  but will there be exception thrown for size not available as we don't unmap it ? need analysis
        //  (writing a day after) turns out we need to free the map, other wise those data remains to be collected by gc
        //  and file rename or file delete doesn't run. found it the hard way :)
        if (map != null) {
            unmap(map);
        }
        map = channel.map(FileChannel.MapMode.READ_ONLY, position, requiredSize);
        mapOffset = (int) position;
    }

    @Override
    public void close() throws IOException {
        if (map != null) {
            unmap(map);
        }
        channel.close();
        randomAccessFile.close();
    }
}

