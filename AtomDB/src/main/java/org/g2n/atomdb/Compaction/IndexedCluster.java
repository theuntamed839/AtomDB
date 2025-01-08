package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Compression.DataCompressionStrategy;
import org.g2n.atomdb.Compression.Lz4Compression;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.MMappedReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.zip.CRC32C;

public class IndexedCluster {
    private static final int NOT_CALCULATED_YET = -1;
    private final int MAX_NUM_OF_ENTRIES_IN_CLUSTER;
    private final CRC32C checksum;
    private final DataCompressionStrategy compression;
    private int totatKVSize = 0;
    private int commonPrefix;
    private final List<KVUnit> entries;
    private static final long DUMMY_CHECKSUM = Long.MIN_VALUE;
    public static final int DUMMY_LOCATION = Integer.MIN_VALUE;

    public IndexedCluster(int clusterSize) {
        this.MAX_NUM_OF_ENTRIES_IN_CLUSTER = clusterSize;
        this.entries = new ArrayList<>(clusterSize);
        this.commonPrefix = NOT_CALCULATED_YET;
        this.checksum = new CRC32C(); // todo using naked crc32c
        this.compression = Lz4Compression.getInstance();
    }

    public void add(KVUnit kv) {
        if (entries.size() >= MAX_NUM_OF_ENTRIES_IN_CLUSTER) {
            throw new IllegalStateException("IndexedCluster is full");
        }

        if (!entries.isEmpty() && DBComparator.byteArrayComparator.compare(kv.getKey(), entries.getLast().getKey()) <= 0) {
            throw new IllegalArgumentException("Got a key that is not greater than the last key");
        }

        entries.add(kv);
        totatKVSize += kv.getUnitSize();
        calculateCommonPrefix(kv.getKey());
    }

    public List<KVUnit> getEntries() {
        return Collections.unmodifiableList(entries);
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

    // 10 long checksum + 11 integer locations + 1 integer commonPrefix + 10 clusters (kvs)
    // n long checksum + n + 1 integer locations + 1 integer commonPrefix + n clusters (kvs)
    public void storeAsBytes(ExpandingByteBuffer writer) throws IOException {
        if (entries.isEmpty()) {
            throw new RuntimeException("Indexed Cluster entries are empty");
        }
        List<Long> checksums = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<byte[]> kvs = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<Integer> locations = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);

        int position = 0;
        for (KVUnit entry : entries) {
            checksums.add(getChecksum(entry.getKey()));
            locations.add(position);

            var block = createBlock(entry);
            byte[] compressed = compression.compress(block.array());

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

    private ByteBuffer createBlock(KVUnit entry) {
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        byte isDelete = entry.getDeletedStatus().value();

        int requiredSize = getRequiredSize(entry);
        ByteBuffer buffer = ByteBuffer.allocate(requiredSize);

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

        if (buffer.remaining() != 0) {
            throw new RuntimeException("Math gone wrong");
        }

        buffer.flip();
        return buffer;
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

    public static void fillQueue(MMappedReader reader, Pointer pointer, byte numberOfKeysInSingleCluster, Queue<KVUnit> queue) throws IOException {
        reader.position((int) (pointer.position() + Long.BYTES * numberOfKeysInSingleCluster)); // skip checksums
        List<Integer> locations = getLocationList(getBytes(reader, Integer.BYTES * (numberOfKeysInSingleCluster + 1)), numberOfKeysInSingleCluster);
        int commonPrefix = reader.getInt();
        ByteBuffer bytes = getBytes(reader, getTotalSizeToReadForKVs(locations));
        DataCompressionStrategy decompressor = Lz4Compression.getInstance();

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

    private static ByteBuffer getBytes(MMappedReader reader, int size) throws IOException {
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
