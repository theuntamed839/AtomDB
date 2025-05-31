package org.g2n.atomdb.search;

import org.g2n.atomdb.Compaction.Pointer;
import org.g2n.atomdb.Compaction.PointerList;
import org.g2n.atomdb.Compression.Lz4Compression;
import org.g2n.atomdb.Constants.DBConstant;
import com.github.benmanes.caffeine.cache.Cache;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.IOReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  we should work on moving respective code to thier respective classes.
 */
public class Finder implements AutoCloseable{
    private final PointerList pointerList;
    private final IOReader reader;
    private final Cache<Pointer, Checksums> checksumsCache;

    public Finder(PointerList pointerList, Cache<Pointer, Checksums> checksumsCache, IOReader reader) {
        // todo
        // we need not mapp the whole fileToWrite rather map only required potion, ie we  dont need header and pointers region
        this.reader = reader;
        this.pointerList = pointerList;
        this.checksumsCache = checksumsCache;
    }

    public KVUnit find(byte[] key, long keyChecksum) throws IOException {
        Pointer pointer = getPointer(key);
        reader.position((int) pointer.position());

        Checksums check = checksumsCache.get(pointer, position -> {
            var checksums = new long[DBConstant.CLUSTER_SIZE];
            for (int i = 0; i < DBConstant.CLUSTER_SIZE; i++) {
                try {
                    checksums[i] = reader.getLong();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new Checksums(checksums);
        });

        int index = getIndex(check, keyChecksum);
        if (index == -1) {
            return null;
        }

        return getLocation(key, keyChecksum, index, (int) pointer.position());
    }

    private int getIndex(Checksums check, long keyChecksum) {
        long[] checksums = check.checksums();
        for (int i = 0; i < checksums.length; i++) {
            if (keyChecksum == checksums[i]) {
                return i;
            }
        }
        return -1;
    }

    private KVUnit getLocation(byte[] key, long keyChecksum, int index, int initialPosition) throws IOException {
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
        if (KVUnit.DeletionStatus.isDeleted(isDeleted)) return new KVUnit(key);
        int valueLength = wrap.getInt();
        byte[] bytes = new byte[valueLength];
        wrap.get(bytes);
        return new KVUnit(key, bytes);
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
