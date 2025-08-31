package io.github.theuntamed839.datastore4j.compaction;

import io.github.theuntamed839.datastore4j.compression.CompressionStrategyFactory;
import io.github.theuntamed839.datastore4j.compression.DataCompressionStrategy;
import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.sstIO.SSTHeader;
import io.github.theuntamed839.datastore4j.db.ExpandingByteBuffer;
import io.github.theuntamed839.datastore4j.db.KVUnit;
import io.github.theuntamed839.datastore4j.sstIO.IOReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/**
 TODO:
    1) We should be storing the common prefix in the cluster itself for easier key construction if key is not known beforehand.
    2) We could also use common prefix for value as well, since these are expected to be bigger in size.
  */

public class IndexedCluster {
    private static final ThreadLocal<Checksum> crc32cThreadLocal = ThreadLocal.withInitial(CRC32C::new);
    private static final int NOT_CALCULATED_YET = -1;
    private final int MAX_NUM_OF_ENTRIES_IN_CLUSTER;
    private final DataCompressionStrategy compression;
    private final boolean shouldUseCommonPrefix; // TODO
    private final Comparator<byte[]> byteArrayComparator;
    private int totalKVSize = 0;
    private int commonPrefix;
    private final List<KVUnit> entries;
    private static final long DUMMY_CHECKSUM = Long.MIN_VALUE;
    public static final int DUMMY_LOCATION = Integer.MIN_VALUE;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public IndexedCluster(SSTHeader header, Comparator<byte[]> byteArrayComparator) {
        this.shouldUseCommonPrefix = header.isShortestCommonPrefixUsed();
        this.byteArrayComparator = byteArrayComparator;
        this.commonPrefix = NOT_CALCULATED_YET;
        this.compression = CompressionStrategyFactory.getCompressionStrategy(header.getCompressionStrategy());
        this.MAX_NUM_OF_ENTRIES_IN_CLUSTER = header.getSingleClusterSize();
        this.entries = new ArrayList<>(this.MAX_NUM_OF_ENTRIES_IN_CLUSTER);
    }

    public void add(KVUnit kv) {
        if (entries.size() >= MAX_NUM_OF_ENTRIES_IN_CLUSTER) {
            throw new IllegalStateException("IndexedCluster is full");
        }

        if (!entries.isEmpty() && byteArrayComparator.compare(kv.getKey(), entries.getLast().getKey()) <= 0) {
            throw new IllegalArgumentException("Got a key that is not greater than the last key");
        }

        entries.add(kv);
        totalKVSize += kv.getUnitSize();
        calculateCommonPrefix(kv.getKey());
    }

    public boolean isFull() {
        return entries.size() >= MAX_NUM_OF_ENTRIES_IN_CLUSTER;
    }

    private void calculateCommonPrefix(byte[] key) {
        if (commonPrefix == NOT_CALCULATED_YET) {
            commonPrefix = key.length;
        } else {
            int newPrefixLength = 0;
            var first = entries.getFirst().getKey();
            int minLength = Math.min(commonPrefix, key.length);
            for (int i = 0; i < minLength; i++) {
                if (first[i] != key[i]) {
                    break; // Stop when a difference is found
                }
                newPrefixLength++;
            }
            commonPrefix = newPrefixLength;
        }
    }

    public byte[] getFirstKey() {
        return entries.getFirst().getKey();
    }

    public byte[] getLastKey() {
        return entries.getLast().getKey();
    }

    // 10 long checksum + 11 integer locations + 1 integer commonPrefix + 10 block (kvs)
    // n long checksum + n + 1 integer locations + 1 integer commonPrefix + n block (kvs)
    public void storeAsBytes(ExpandingByteBuffer writer) throws IOException {
        if (entries.isEmpty()) {
            throw new RuntimeException("Indexed Cluster entries are empty");
        }
        List<Long> checksums = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<byte[]> kvs = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<Integer> locations = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER + 1);

        int position = 0;
        for (KVUnit entry : entries) {
            checksums.add(getChecksum(entry.getKey()));
            locations.add(position);

            byte[] compressed = compression.compress(createBlock(entry));

            kvs.add(compressed);

            position += compressed.length;
        }
        locations.add(position); // will be used to get the last block data.

        if (checksums.size() != MAX_NUM_OF_ENTRIES_IN_CLUSTER) {
            fillDummyData(checksums, locations);
        }
        checksums.forEach(writer::putLong);
        locations.forEach(writer::putInt);
        writer.putInt(commonPrefix);
        kvs.forEach(writer::put);
    }

    private byte[] createBlock(KVUnit entry) {
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        byte marker = entry.getTombStoneValue();

        int requiredSize = getRequiredSize(entry);
        var buffer = bufferThreadLocal.get();//ByteBuffer.allocate(requiredSize);
        buffer.clear();

        Checksum checksum = crc32cThreadLocal.get();
        checksum.reset();
        checksum.update(key, commonPrefix, key.length - commonPrefix);
        checksum.update(marker);
        buffer.putInt(key.length - commonPrefix)
                .put(key, commonPrefix, key.length - commonPrefix)
                .put(marker);

        if (!entry.isTombStone()) {
            buffer.putInt(value.length)
                    .put(value);
            checksum.update(value);
        }

        buffer.putLong(checksum.getValue());

        if (buffer.position() != requiredSize) {
            throw new RuntimeException("Math gone wrong");
        }

        byte[] array = new byte[buffer.position()];
        buffer.flip();
        buffer.get(array);
        return array;
    }

    private void fillDummyData(List<Long> checksums, List<Integer> locations) {
        for (int i = checksums.size(); i < MAX_NUM_OF_ENTRIES_IN_CLUSTER; i++) {
            checksums.add(DUMMY_CHECKSUM);
            locations.add(DUMMY_LOCATION);
        }
    }

    private long getChecksum(byte[] key) {
        Checksum checksum = crc32cThreadLocal.get();
        checksum.reset();
        checksum.update(key);
        return checksum.getValue();
    }

    private int getRequiredSize(KVUnit unit) {
        byte[] key = unit.getKey();
        byte[] value = unit.getValue();
        return Integer.BYTES + key.length + Byte.BYTES
                + (!unit.isTombStone() ? Integer.BYTES + value.length : 0)
                + Long.BYTES // checksum
                - commonPrefix;
    }

    public int getTotalSize() {
        return totalKVSize;
    }

    public int getNumberOfEntries() {
        return entries.size();
    }


    public static IndexedCluster getNextCluster(Iterator<KVUnit> iterator, SSTHeader sstHeader, Comparator<byte[]> comparator) {
        var cluster = new IndexedCluster(sstHeader, comparator);
        while (iterator.hasNext() && !cluster.isFull()) {
            cluster.add(iterator.next());
        }
        return cluster;
    }

    public static void fillQueue(IOReader reader,
                                 Pointer pointer,
                                 byte numberOfKeysInSingleCluster,
                                 DBConstant.COMPRESSION_TYPE compressionStrategy,
                                 Queue<KVUnit> queue) throws IOException {
        var decompressor = CompressionStrategyFactory.getCompressionStrategy(compressionStrategy);
        reader.position(pointer.position() + Long.BYTES * numberOfKeysInSingleCluster); // skip checksums

        List<Integer> locations = getLocationList(reader, numberOfKeysInSingleCluster);
        int commonPrefix = reader.getInt();
        long beforeReadingKV = reader.position();
        int kvCount = getTotalKVsInCluster(locations);
        for (int i = 0; i < kvCount; i++) {
            var block = decompressBlock(locations, i, reader, decompressor);
            queue.add(parseKV(pointer, block, commonPrefix));
        }

        if (queue.size() != kvCount) {
            throw new IllegalStateException("Queue size does not match expected number of KVs in cluster");
        }

        if (reader.position() != locations.get(kvCount) + beforeReadingKV) {
            throw new IllegalStateException("Reader position does not match expected end of cluster" + reader.position() + " != " + (locations.get(kvCount) + beforeReadingKV));
        }
    }

    private static int getTotalKVsInCluster(List<Integer> locations) {
        for (int i = 0; i < locations.size(); i++) {
            if (locations.get(i) == DUMMY_LOCATION) {
                return i - 1;
            }
        }
        return locations.size() - 1;
    }

    private static KVUnit parseKV(Pointer pointer, ByteBuffer block, int commonPrefix) {
        int keyLength = block.getInt();

        byte[] key = new byte[keyLength + commonPrefix];
        System.arraycopy(pointer.key(), 0, key, 0, commonPrefix);
        block.get(key, commonPrefix, keyLength);

        byte marker = block.get();

        if (KVUnit.isTombStone(marker)) {
            return new KVUnit(key);
        } else {
            int valueLength = block.getInt();
            byte[] value = new byte[valueLength];
            block.get(value);
            return new KVUnit(key, value);
        }
    }

    private static ByteBuffer decompressBlock(List<Integer> locations, int index, IOReader bytes, DataCompressionStrategy decompressor) throws IOException {
        byte[] block = new byte[locations.get(index + 1) - locations.get(index)];
        bytes.get(block);
        byte[] decompress = decompressor.decompress(block);
        return ByteBuffer.wrap(decompress);
    }

    private static List<Integer> getLocationList(IOReader wrap, int sizeOfCluster) throws IOException {
        List<Integer> locations = new ArrayList<>();
        for (int i = 0; i < sizeOfCluster + 1; i++) {
            locations.add(wrap.getInt());
        }
        return locations;
    }
}
