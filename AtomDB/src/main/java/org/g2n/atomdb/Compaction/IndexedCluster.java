package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Compression.DataCompressionStrategy;
import org.g2n.atomdb.Compression.Lz4Compression;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.MMappedReader;
import org.g2n.atomdb.sstIo.ChannelBackedWriter;

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
    private static final int DUMMY_LOCATION = Integer.MIN_VALUE;

    public IndexedCluster(int clusterSize) {
        this.MAX_NUM_OF_ENTRIES_IN_CLUSTER = clusterSize;
        this.entries = new ArrayList<>(clusterSize);
        this.commonPrefix = NOT_CALCULATED_YET;
        this.checksum = new CRC32C(); // todo using naked crc32c
        this.compression = Lz4Compression.getInstance();
    }

    public static Cluster readSimpleCluster(MMappedReader reader, Pointer pointer, byte numberOfKeysInSingleCluster) throws IOException {
        reader.position((int) (pointer.position() + Long.BYTES * numberOfKeysInSingleCluster));
        List<Integer> locations = getLocationList(getBytes(reader, Integer.BYTES * (numberOfKeysInSingleCluster + 1)), numberOfKeysInSingleCluster);
        int commonPrefix = reader.getInt();
        ByteBuffer bytes = getBytes(reader, getTotalSizeToReadForKVs(locations));

        List<KVUnit> units = new ArrayList<>();
        for (int i = 0; i < numberOfKeysInSingleCluster && bytes.hasRemaining(); i++) {
            byte[] block = new byte[locations.get(i + 1) - locations.get(i)];
            bytes.get(block);
            byte[] decompress = Lz4Compression.getInstance().decompress(block);
            var wrap = ByteBuffer.wrap(decompress);
            int keyLength = wrap.getInt();

            byte[] key = new byte[keyLength + commonPrefix];
            System.arraycopy(pointer.key(), 0, key, 0, commonPrefix);
            wrap.get(key, commonPrefix, keyLength);

            var isDeleted = KVUnit.DeletionStatus.of(wrap.get());
            if (KVUnit.DeletionStatus.DELETED == isDeleted) {
                units.add(new KVUnit(key));
            } else {
                int valueLength = wrap.getInt();
                byte[] value = new byte[valueLength];
                wrap.get(value);
                units.add(new KVUnit(key, value));
            }
        }
        return new Cluster(units);
    }

    public static void fillQueue(MMappedReader reader, Pointer pointer, byte numberOfKeysInSingleCluster, Queue<KVUnit> queue) throws IOException {
        reader.position((int) (pointer.position() + Long.BYTES * numberOfKeysInSingleCluster));
        List<Integer> locations = getLocationList(getBytes(reader, Integer.BYTES * (numberOfKeysInSingleCluster + 1)), numberOfKeysInSingleCluster);
        int commonPrefix = reader.getInt();
        ByteBuffer bytes = getBytes(reader, getTotalSizeToReadForKVs(locations));

        for (int i = 0; i < numberOfKeysInSingleCluster && bytes.hasRemaining(); i++) {
            byte[] block = new byte[locations.get(i + 1) - locations.get(i)];
            bytes.get(block);
            byte[] decompress = Lz4Compression.getInstance().decompress(block);
            var wrap = ByteBuffer.wrap(decompress);
            int keyLength = wrap.getInt();

            byte[] key = new byte[keyLength + commonPrefix];
            System.arraycopy(pointer.key(), 0, key, 0, commonPrefix);
            wrap.get(key, commonPrefix, keyLength);

            var isDeleted = KVUnit.DeletionStatus.of(wrap.get());
            if (KVUnit.DeletionStatus.DELETED == isDeleted) {
                queue.add(new KVUnit(key));
            } else {
                int valueLength = wrap.getInt();
                byte[] value = new byte[valueLength];
                wrap.get(value);
                queue.add(new KVUnit(key, value));
            }
        }
    }

    public Cluster read(MMappedReader reader, Pointer pointer) throws IOException {
        reader.position((int) (pointer.position() + Long.BYTES * MAX_NUM_OF_ENTRIES_IN_CLUSTER));
        List<Integer> locations = getLocationList(getBytes(reader, Integer.BYTES * (MAX_NUM_OF_ENTRIES_IN_CLUSTER + 1)), MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        int commonPrefix = reader.getInt();
        ByteBuffer bytes = getBytes(reader, getTotalSizeToReadForKVs(locations));

        List<KVUnit> units = new ArrayList<>();
        for (int i = 0; i < MAX_NUM_OF_ENTRIES_IN_CLUSTER && bytes.hasRemaining(); i++) {
            byte[] block = new byte[locations.get(i + 1) - locations.get(i)];
            bytes.get(block);
            byte[] decompress = compression.decompress(block);
            var wrap = ByteBuffer.wrap(decompress);
            int keyLength = wrap.getInt();

            byte[] key = new byte[keyLength + commonPrefix];
            System.arraycopy(pointer.key(), 0, key, 0, commonPrefix);
            wrap.get(key, commonPrefix, keyLength);

            var isDeleted = KVUnit.DeletionStatus.of(wrap.get());
            if (KVUnit.DeletionStatus.DELETED == isDeleted) {
                units.add(new KVUnit(key));
            } else {
                int valueLength = wrap.getInt();
                byte[] value = new byte[valueLength];
                wrap.get(value);
                units.add(new KVUnit(key, value));
            }
        }
        return new Cluster(units);
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
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        return wrap;
    }

    private static List<Integer> getLocationList(ByteBuffer wrap, int sizeOfCluster) {
        List<Integer> locations = new ArrayList<>();
        for (int i = 0; i < sizeOfCluster + 1; i++) {
            locations.add(wrap.getInt());
        }
        return locations;
    }

    private List<Long> getChecksumList(ByteBuffer wrap) {
        List<Long> checksums = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        for (int i = 0; i < MAX_NUM_OF_ENTRIES_IN_CLUSTER; i++) {
            checksums.add(wrap.getLong());
        }
        return checksums;
    }

    public void add(KVUnit kv) {
        if (entries.size() >= MAX_NUM_OF_ENTRIES_IN_CLUSTER) {
            throw new IllegalStateException("IndexedCluster is full");
        }

        // todo remove this check
        List<byte[]> list = entries.stream().map(KVUnit::getKey).toList();
        if (Math.abs(Collections.binarySearch(list, kv.getKey(), DBComparator.byteArrayComparator)) < entries.size()) {
            System.out.println("Math.abs(Collections.binarySearch(validatorList, kvUnit.getKey(), DBComparator.byteArrayComparator))" + Math.abs(Collections.binarySearch(list, kv.getKey(), DBComparator.byteArrayComparator)));
            System.out.println("validatorList.size()" + list.size());
            throw new RuntimeException("Got a lesser key than previous key");
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
            var first = entries.get(0).getKey();
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

    public KVUnit getLastKV() {
        return entries.getLast();
    }

    // 10 long checksum + 11 integer locations + 1 integer commonPrefix + 10 clusters (kvs)
    // n long checksum + n + 1 integer locations + 1 integer commonPrefix + n clusters (kvs)
    public void storeAsBytes(ChannelBackedWriter writer) throws IOException {
        if (entries.isEmpty()) {
            throw new RuntimeException("Indexed Cluster entries are empty");
        }
        List<Long> checksums = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<byte[]> kvs = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<Integer> locations = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);

        int location = 0;
        for (KVUnit entry : entries) {
            checksums.add(getChecksum(entry.getKey()));
            locations.add(location);

            var block = createBlock(entry);
            byte[] compressed = compression.compress(block.array());

            kvs.add(compressed);

            location += compressed.length;
        }
        locations.add(location); // will be used to get the last block data.

        if (checksums.size() != MAX_NUM_OF_ENTRIES_IN_CLUSTER) {
            fillDummyData(checksums, locations);
        }
        checksums.forEach(writer::putLong);
        locations.forEach(writer::putInt);
        writer.putInt(commonPrefix);
        kvs.forEach(writer::putBytes);
    }

    public void storeAsBytes(ExpandingByteBuffer writer) throws IOException {
        if (entries.isEmpty()) {
            throw new RuntimeException("Indexed Cluster entries are empty");
        }
        List<Long> checksums = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<byte[]> kvs = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);
        List<Integer> locations = new ArrayList<>(MAX_NUM_OF_ENTRIES_IN_CLUSTER);

        int location = 0;
        for (KVUnit entry : entries) {
            checksums.add(getChecksum(entry.getKey()));
            locations.add(location);

            var block = createBlock(entry);
            byte[] compressed = compression.compress(block.array());

            kvs.add(compressed);

            location += compressed.length;
        }
        locations.add(location); // will be used to get the last block data.

        if (checksums.size() != MAX_NUM_OF_ENTRIES_IN_CLUSTER) {
            fillDummyData(checksums, locations);
        }
        checksums.forEach(writer::putLong);
        locations.forEach(writer::putInt);
        writer.putInt(commonPrefix);
        kvs.forEach(writer::put);
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

    private ByteBuffer createBlock(KVUnit entry) {
        // todo code improvement, can be made more modular.
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        byte isDelete = entry.getDeletedStatus().value();

        int requiredSize = getRequiredSize(entry);
        // todo performance improvement, use a temp buffer.
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
}
