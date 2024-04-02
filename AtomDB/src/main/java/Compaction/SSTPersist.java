package Compaction;

import Constants.DBConstant;
import Level.Level;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.KVUnit;
import sstIo.BufferedMMappedWriter;
import sstIo.ChannelBackedWriter;
import sstIo.SSTHeader;
import sstIo.SSTKeyRange;

import java.io.File;
import java.util.Iterator;

/**
 * todo
 *  1. we need not calculate number of entries for the mem backed sst.
 *  2. can we have parallel stream.
 */
public class SSTPersist {

    private final File file;
    private final Iterator<KVUnit> iterator;
    private final SSTKeyRange keyRange;
    private final int upperLimitOfEntries;
    private final int clusterSize;
    private final SSTHeader sstHeader;
    private PointerList pointers;

    public SSTPersist(File file,
                      Iterator<KVUnit> iterator,
                      SSTKeyRange keyRange,
                      int upperLimitOfEntries,
                      int clusterSize) {
        this.file = file;
        this.iterator = iterator;
        this.keyRange = keyRange;
        this.upperLimitOfEntries = upperLimitOfEntries;
        this.clusterSize = clusterSize;
        this.sstHeader = createHeader();
        write();
    }

    private SSTHeader createHeader() {
        return new SSTHeader(
                DBConstant.SST_VERSION,
                Level.LEVEL_ZERO,
                DBConstant.CRC32C_CHECKSUM_TYPE,
                DBConstant.LZ4_COMPRESSION_TYPE,
                DBConstant.CLUSTER_SIZE,
                DBConstant.SHORTEST_COMMON_PREFIX_USED,
                keyRange);
    }

    private void write() {
        try (
                ChannelBackedWriter writer = new BufferedMMappedWriter(file);
        ) {

            BloomFilter<byte[]> filter = BloomFilter.create(
                    Funnels.byteArrayFunnel(),
                    upperLimitOfEntries,
                    0.01);
            FilterAddingIterator customIterator = new FilterAddingIterator(iterator, filter);
            this.pointers = new PointerList(upperLimitOfEntries);

            // writing
            // header
            writer.putInt(sstHeader.totalHeaderSize());
            sstHeader.storeAsBytes(writer);
            // middle block
            while(customIterator.hasNext()) {
                var indexedCluster = getNextCluster(customIterator);
                pointers.add(new Pointer(indexedCluster.getFirstKey(), writer.position()));
                indexedCluster.storeAsBytes(writer);
            }
            // footer
            sstHeader.setEntries(customIterator.getCount());
            sstHeader.setFilterPosition(writer.position());
            filter.writeTo(writer);

            sstHeader.setPointersPosition(writer.position());
            pointers.storeAsBytes(writer);

            writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading file.

            sstHeader.writeRemaining(writer);
            sstHeader.close(); // important to close
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IndexedCluster getNextCluster(Iterator<KVUnit> customIterator) {
        var cluster = new IndexedCluster(clusterSize);
        for (int i = 0; i < clusterSize && customIterator.hasNext(); i++) {
            KVUnit current = customIterator.next();
            cluster.add(current);
        }
        return cluster;
    }

    public PointerList getCheckPoints() {
        return pointers;
    }
}
