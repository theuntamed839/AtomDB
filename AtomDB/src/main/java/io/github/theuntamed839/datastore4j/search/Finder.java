package io.github.theuntamed839.datastore4j.search;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.theuntamed839.datastore4j.compaction.Pointer;
import io.github.theuntamed839.datastore4j.compaction.PointerList;
import io.github.theuntamed839.datastore4j.compression.CompressionStrategyFactory;
import io.github.theuntamed839.datastore4j.compression.DataCompressionStrategy;
import com.github.benmanes.caffeine.cache.Cache;
import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import io.github.theuntamed839.datastore4j.sstIO.IOReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantLock;

/**
 *TODO:
 * 1. we need not map the whole file, as we only reading the Mid section only, this would save the overhead of mapping and other stuff.
 *
 */
public class Finder implements AutoCloseable{
    private final PointerList pointerList;
    private final Comparator<byte[]> byteArrayComparator;
    private final IOReader reader;
    private final Cache<Pointer, Checksums> checksumsCache;
    private final byte singleClusterSize;
    private final DataCompressionStrategy compressionStrategy;
    private final ReentrantLock lock = new ReentrantLock();

    public Finder(PointerList pointerList, IOReader reader, byte singleClusterSize, DBConstant.COMPRESSION_TYPE compressionStrategy, Comparator<byte[]> byteArrayComparator) {
        this.reader = reader;
        this.pointerList = pointerList;
        this.byteArrayComparator = byteArrayComparator;
        this.checksumsCache = Caffeine.newBuilder()
                .softValues() //todo should we have hard limit instead ?
                .build();
        this.singleClusterSize = singleClusterSize;
        this.compressionStrategy = CompressionStrategyFactory.getCompressionStrategy(compressionStrategy);
    }

    public KVUnit find(byte[] key, long keyChecksum) throws IOException {
        lock.lock();
        try {
            var pointer = getPointerToCluster(key);
            if (pointer == null) {
                return null; // No pointer found for the key.
            }
            reader.position(pointer.position());

            var checksums = checksumsCache.get(pointer, _pointer -> {
                var arr = new long[this.singleClusterSize];
                for (int i = 0; i < this.singleClusterSize; i++) {
                    try {
                        arr[i] = reader.getLong();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return new Checksums(arr);
            }).checksums();

            for (int i = 0; i < checksums.length; i++) {
                if (keyChecksum == checksums[i]) {
                    var kv = getLocation(pointer, i);
                    if (byteArrayComparator.compare(key, kv.getKey()) == 0) {
                        return kv;
                    }
                }
            }
            return null;
        }finally {
            lock.unlock();
        }
    }

    private KVUnit getLocation(Pointer point, int index) throws IOException {
        // Moving to location block
        long afterChecksums = point.position() + Long.BYTES * this.singleClusterSize;
        long keysLocatingIndexPos = afterChecksums + (long) index * Integer.BYTES;
        long skipChecksumsAndLocations = afterChecksums + (this.singleClusterSize + 1) *  ((long) Integer.BYTES);
        long skipClusterMetaData = skipChecksumsAndLocations + Integer.BYTES;

        reader.position(keysLocatingIndexPos);
        int internalKeyLocation = reader.getInt();
        int internalNextKeyLocation = reader.getInt();


        reader.position(skipChecksumsAndLocations);
        int commonPrefix = reader.getInt();

        long keyLocation = skipClusterMetaData + internalKeyLocation;
        reader.position(keyLocation);

        int blockSizeToRead = internalNextKeyLocation - internalKeyLocation;
        var block = new byte[blockSizeToRead];
        reader.read(block);

        return getKvUnit(point.key(), commonPrefix, block);
    }

    private KVUnit getKvUnit(byte[] pointerKey, int commonPrefix, byte[] block) throws IOException {
        byte[] decompress = compressionStrategy.decompress(block);
        var wrapper = ByteBuffer.wrap(decompress);
        int keyLength = wrapper.getInt();
        byte[] foundKey = new byte[keyLength + commonPrefix];

        System.arraycopy(pointerKey, 0, foundKey, 0, commonPrefix);
        wrapper.get(foundKey, commonPrefix, keyLength);

        byte marker = wrapper.get();
        if (KVUnit.isTombStone(marker)) {
            return new KVUnit(foundKey);
        }
        int valueLength = wrapper.getInt();
        byte[] foundValue = new byte[valueLength];
        wrapper.get(foundValue);
        return new KVUnit(foundKey, foundValue);
    }

    private Pointer getPointerToCluster(byte[] key) {
        int index = getPointerIndex(key);

        if (index < 0) {
           return null;
        }

        if (index == pointerList.size() -1) {
            index--; // finding the last element.
        }
        return pointerList.get(index);
    }

    private int getPointerIndex(byte[] key) {
        int l = 0, h = pointerList.size() - 1;
        while(l <= h) {
            int mid = (l + h) >>> 1;
            Pointer midPointer = pointerList.get(mid);
            int compare = byteArrayComparator.compare(midPointer.key(), key);
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
        lock.lock();
        try {
            reader.close();
            checksumsCache.invalidateAll();
        } finally {
            lock.unlock();
        }
    }
}
