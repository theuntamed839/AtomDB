package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Compression.CompressionStrategyFactory;
import org.g2n.atomdb.Compression.DataCompressionStrategy;
import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.SSTIO.SSTHeader;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.SSTIO.IOReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.zip.CRC32C;

import static org.g2n.atomdb.db.DBComparator.byteArrayComparator;

/**
 TODO:
    1) What if same checksum occurs for 2 different keys?
    2) We should be storing the common prefix in the cluster itself for easier key construction if key is not known beforehand.
    3) We could also use common prefix for value as well, since these are expected to be bigger in size.
  */



public class IndexedCluster {
    private static final int NOT_CALCULATED_YET = -1;
    private final int MAX_NUM_OF_ENTRIES_IN_CLUSTER;
    private final CRC32C checksum;
    private final DataCompressionStrategy compression;
    private final boolean shouldUseCommonPrefix; // TODO
    private int totatKVSize = 0;
    private int commonPrefix;
    private final List<KVUnit> entries;
    private static final long DUMMY_CHECKSUM = Long.MIN_VALUE;
    public static final int DUMMY_LOCATION = Integer.MIN_VALUE;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public IndexedCluster(SSTHeader header) {
        this.shouldUseCommonPrefix = header.isShortestCommonPrefixUsed();
        this.commonPrefix = NOT_CALCULATED_YET;
        this.checksum = new CRC32C(); // todo using naked crc32c
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
        totatKVSize += kv.getUnitSize();
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
        byte isDelete = entry.getDeletedStatus().value();

        int requiredSize = getRequiredSize(entry);
        var buffer = bufferThreadLocal.get();//ByteBuffer.allocate(requiredSize);
        buffer.clear();

        checksum.reset();
        checksum.update(key, commonPrefix, key.length - commonPrefix);
        checksum.update(isDelete);
        buffer.putInt(key.length - commonPrefix)
                .put(key, commonPrefix, key.length - commonPrefix)
                .put(isDelete);

        if (!entry.isDeleted()) {
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
        checksum.reset();
        checksum.update(key);
        return checksum.getValue();
    }

    private int getRequiredSize(KVUnit unit) {
        byte[] key = unit.getKey();
        byte[] value = unit.getValue();
        return Integer.BYTES + key.length + Byte.BYTES
                + (!unit.isDeleted() ? Integer.BYTES + value.length : 0)
                + Long.BYTES // checksum
                - commonPrefix;
    }

    public int getTotalSize() {
        return totatKVSize;
    }

    public int getNumberOfEntries() {
        return entries.size();
    }


    public static IndexedCluster getNextCluster(Iterator<KVUnit> iterator, SSTHeader sstHeader) {
        var cluster = new IndexedCluster(sstHeader);
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
        reader.position((int) (pointer.position() + Long.BYTES * numberOfKeysInSingleCluster)); // skip checksums
        List<Integer> locations = getLocationList(getBytes(reader, Integer.BYTES * (numberOfKeysInSingleCluster + 1)), numberOfKeysInSingleCluster);
        int commonPrefix = reader.getInt();
        ByteBuffer bytes = getBytes(reader, getTotalSizeToReadForKVs(locations));
        DataCompressionStrategy decompressor = CompressionStrategyFactory.getCompressionStrategy(compressionStrategy);

        for (int i = 0; bytes.hasRemaining(); i++) {
            var block = decompressBlock(locations, i, bytes, decompressor);
            queue.add(parseKV(pointer, block, commonPrefix));
        }
    }

    private static KVUnit parseKV(Pointer pointer, ByteBuffer block, int commonPrefix) {
        int keyLength = block.getInt();

        byte[] key = new byte[keyLength + commonPrefix];
        System.arraycopy(pointer.key(), 0, key, 0, commonPrefix);
        block.get(key, commonPrefix, keyLength);

        byte isDeleted = block.get();

        if (KVUnit.DeletionStatus.DELETED == KVUnit.DeletionStatus.of(isDeleted)) {
            return new KVUnit(key);
        } else {
            int valueLength = block.getInt();
            byte[] value = new byte[valueLength];
            block.get(value);
            return new KVUnit(key, value);
        }
    }

    private static ByteBuffer decompressBlock(List<Integer> locations, int index, ByteBuffer bytes, DataCompressionStrategy decompressor) throws IOException {
        byte[] block = new byte[locations.get(index + 1) - locations.get(index)];
        bytes.get(block);
        byte[] decompress = decompressor.decompress(block);
        return ByteBuffer.wrap(decompress);
    }

    private static int getTotalSizeToReadForKVs(List<Integer> locations) {
        for (int i = 0; i < locations.size(); i++) {
            if (locations.get(i) == DUMMY_LOCATION) {
                return locations.get(i - 1);
            }
        }
        return locations.getLast();
    }

    private static ByteBuffer getBytes(IOReader reader, int size) throws IOException {
        byte[] bytes = new byte[size];
        reader.read(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static List<Integer> getLocationList(ByteBuffer wrap, int sizeOfCluster) {
        List<Integer> locations = new ArrayList<>();
        for (int i = 0; i < sizeOfCluster + 1; i++) {
            locations.add(wrap.getInt());
        }
        return locations;
    }
}
