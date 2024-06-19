package search;

import Compaction.Pointer;
import Compaction.PointerList;
import Compression.Lz4Compression;
import Constants.DBConstant;
import db.DBComparator;
import db.KVUnit;
import sst.ValueUnit;
import sstIo.MMappedReader;
import sstIo.ChannelBackedReader;
import util.MaxMinAvg;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  we should work on moving respective code to thier respective classes.
 */
public class Finder implements AutoCloseable{

    private final File file;
    private final PointerList pointerList;
    private final ChannelBackedReader reader;
    private final MaxMinAvg pointerTime;

    public Finder(File file, PointerList pointerList) throws IOException {
        this.file = file;
        this.reader = new MMappedReader(file);
        this.pointerList = pointerList;
        this.pointerTime = new MaxMinAvg();
    }

    public ValueUnit find(byte[] key, long keyChecksum) throws IOException {
        Pointer pointer = getPointer(key);
        reader.position((int) pointer.position());
        ValueUnit location = getLocation(key, keyChecksum);
        return location;
    }

    private ValueUnit getLocation(byte[] key, long keyChecksum) throws IOException {
        int initialPosition = (int) reader.position();
        int index = -1;
        for (int i = 0; i < DBConstant.CLUSTER_SIZE; i++) {
            long check = reader.getLong();
            if (keyChecksum == check) {
                index = i;
            }
        }

        if (index == -1) {
            return null;
        }

        // directly moving to location block
        reader.position( initialPosition + Long.BYTES * DBConstant.CLUSTER_SIZE + index * Integer.BYTES);
        int keyLocation = reader.getInt();
        int nextKeyLocation = reader.getInt();
        reader.position(initialPosition + Long.BYTES * DBConstant.CLUSTER_SIZE + (DBConstant.CLUSTER_SIZE + 1) * Integer.BYTES);
        // todo use the common prefix, to maybe validate.
        int commonPrefix = reader.getInt();
        reader.position((int) (initialPosition +
                Long.BYTES * DBConstant.CLUSTER_SIZE +
                (DBConstant.CLUSTER_SIZE + 1) * Integer.BYTES
                + Integer.BYTES + keyLocation));

        int blockSizeToRead = nextKeyLocation - keyLocation;
        var block = new byte[blockSizeToRead];
        reader.getBytes(block);
        byte[] decompress = Lz4Compression.getInstance().decompress(block);
        var wrap = ByteBuffer.wrap(decompress);
        int keyLength = wrap.getInt();
        wrap.position(wrap.position() + keyLength);
        byte isDeleted = wrap.get();
        if (isDeleted == KVUnit.DELETE) return new ValueUnit(ValueUnit.DELETE);

        int valueLength = wrap.getInt();
        byte[] bytes = new byte[valueLength];
        wrap.get(bytes);
        return new ValueUnit(bytes, ValueUnit.ADDED);
    }

    private Pointer getPointer(byte[] key) {
        int index = getCluster(key);
        if (index == pointerList.size() -1) {
            index--; // finding the last element.
        }
        return pointerList.get(index);
    }

    private int getCluster(byte[] key) {
        int l = 0, h = pointerList.size() - 1;
        while(l <= h) {
            int mid = (l + h) >>> 1;
            Pointer midPointer = pointerList.get(mid);
            int compare = DBComparator.byteArrayComparator.compare(midPointer.key(), key);
            if (compare < 0){
                l = mid + 1;
            }
            else if (compare > 0) {
                h = mid - 1;
            }
            else {
                return mid;
            }
        }
        return h;
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
