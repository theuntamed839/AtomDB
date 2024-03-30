package Compaction;

import Compression.DataCompressionStrategy;
import Compression.Lz4Compression;
import db.KVUnit;
import sstIo.ChannelBackedWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

public class IndexedCluster {
    private static final int NOT_CALCULATED_YET = -1;
    private final int clusterSize;
    private final CRC32C checksum;
    private final DataCompressionStrategy compression;
    private int commonPrefix;
    private final List<KVUnit> entries;
    public IndexedCluster(int clusterSize) {
        this.clusterSize = clusterSize;
        this.entries = new ArrayList<>(clusterSize);
        this.commonPrefix = NOT_CALCULATED_YET;
        this.checksum = new CRC32C(); // todo using naked crc32c
        this.compression = Lz4Compression.getInstance();
    }

    public void add(KVUnit kv) {
        if (entries.size() >= clusterSize) {
            throw new IllegalStateException("IndexedCluster is full");
        }
        entries.add(kv);
        calculateCommonPrefix(kv.getKey());
    }

    private void calculateCommonPrefix(byte[] key) {
        if (commonPrefix == NOT_CALCULATED_YET) {
            commonPrefix = key.length;
        }
        else {
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

    public void storeAsBytes(ChannelBackedWriter writer) throws IOException {
        List<Long> checksums = new ArrayList<>(clusterSize);
        List<byte[]> kvs = new ArrayList<>(clusterSize);
        List<Integer> locations = new ArrayList<>(clusterSize);

        int location = 0;
        for (KVUnit entry : entries) {
            checksums.add(getChecksum(entry.getKey()));
            locations.add(location);

            var block = createBlock(entry);
            byte[] compressed = compression.compress(block.array());

            kvs.add(compressed);

            location += compressed.length;
        }

        checksums.forEach(writer::putLong);
        locations.forEach(writer::putInt);
        writer.putInt(commonPrefix);
        kvs.forEach(writer::putBytes);
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
        byte isDelete = entry.getIsDelete();

        int requiredSize = getRequiredSize(key, value, isDelete);

        ByteBuffer buffer = ByteBuffer.allocate(requiredSize);

        checksum.reset();
        checksum.update(key, commonPrefix, key.length - commonPrefix);
        checksum.update(isDelete);

        buffer.putInt(key.length - commonPrefix)
                .put(key, commonPrefix, key.length - commonPrefix)
                .put(isDelete);

        if (isDelete != KVUnit.DELETE) {
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

    private int getRequiredSize(byte[] key, byte[] value, byte isDelete) {
        return Integer.BYTES + key.length + Byte.BYTES
                + (isDelete != KVUnit.DELETE ? Integer.BYTES + value.length : 0)
                + Long.BYTES // checksum
                - commonPrefix;
    }
}
