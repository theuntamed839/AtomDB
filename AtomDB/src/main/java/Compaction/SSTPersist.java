package Compaction;

import Constants.DBConstant;
import Level.Level;
import Table.SSTInfo;
import Table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.KVUnit;
import sstIo.BufferedMMappedWriter;
import sstIo.ChannelBackedWriter;
import sstIo.SSTHeader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * todo
 *  1. we need not calculate number of entries for the mem backed sst.
 *  2. can we have parallel stream.
 */
public class SSTPersist {

    private final Iterator<KVUnit> iterator;
    private int upperLimitOfEntries;
    private final int clusterSize;
    private SSTHeader sstHeader;
    private final Level level;
    private File currentFile;
    private final Table table;
    private PointerList pointers;

    public SSTPersist(Level level,
                      Iterator<KVUnit> iterator,
                      int upperLimitOfEntries,
                      int clusterSize, Table table) throws IOException {
        this.level = level;
        this.iterator = iterator;
        this.upperLimitOfEntries = upperLimitOfEntries;
        this.clusterSize = clusterSize;
        this.table = table;
        this.sstHeader = createHeader();
        this.currentFile = table.getNewSST(level);
    }

    public SSTPersist(Level level, CollectiveSStIterator iterator, int clusterSize, Table table) throws IOException {
        this.level = level;
        this.iterator = iterator;
        this.clusterSize = clusterSize;
        this.table = table;
    }

    private SSTHeader createHeader() {
        return new SSTHeader(
                DBConstant.SST_VERSION,
                level,
                DBConstant.CRC32C_CHECKSUM_TYPE,
                DBConstant.LZ4_COMPRESSION_TYPE,
                DBConstant.CLUSTER_SIZE,
                DBConstant.SHORTEST_COMMON_PREFIX_USED);
    }

    public SSTInfo write() {
        try (
                ChannelBackedWriter writer = new BufferedMMappedWriter(currentFile);
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
            IndexedCluster indexedCluster = null;
            while(customIterator.hasNext()) {
                indexedCluster = getNextCluster(customIterator);
                pointers.add(new Pointer(indexedCluster.getFirstKey(), writer.position()));
                indexedCluster.storeAsBytes(writer);
            }
            pointers.add(new Pointer(indexedCluster.getLastKey(), Math.negateExact(writer.position())));
            // footer
            sstHeader.setEntries(customIterator.getCount());
            sstHeader.setFilterPosition(writer.position());
            filter.writeTo(writer);

            sstHeader.setPointersPosition(writer.position());
            pointers.storeAsBytes(writer);

            writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading file.

            sstHeader.writeRemaining(writer);
            sstHeader.close(); // important to close
            var sstInfo = new SSTInfo(currentFile, sstHeader, pointers, filter);
            table.addSST(level, sstInfo);
            return sstInfo;
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

    public void writeBigLevel() throws IOException {
        while(iterator.hasNext()) {
            idk(table.getNewSST(level));
        }
    }

    private void idk(File file) {
        try (
                ChannelBackedWriter writer = new BufferedMMappedWriter(file);
        ) {
            BloomFilter<byte[]> filter = BloomFilter.create(
                    Funnels.byteArrayFunnel(),
                    100000,
                    0.01);
            FilterAddingIterator customIterator = new FilterAddingIterator(iterator, filter);
            var pointerList = new PointerList();
            var header = createHeader();
            // writing
            // header
            writer.putInt(header.totalHeaderSize());
            header.storeAsBytes(writer);
            // middle block
            IndexedCluster indexedCluster = null;
            while(customIterator.hasNext() && customIterator.getTotalKVSize() < DBConstant.SST_FILE_SIZE) {
                indexedCluster = getNextCluster(customIterator);
                pointerList.add(new Pointer(indexedCluster.getFirstKey(), writer.position()));
                indexedCluster.storeAsBytes(writer);
            }
            pointerList.add(new Pointer(indexedCluster.getLastKey(), Math.negateExact(writer.position())));
            // footer
            header.setEntries(customIterator.getCount());
            header.setFilterPosition(writer.position());
            filter.writeTo(writer);

            header.setPointersPosition(writer.position());
            pointerList.storeAsBytes(writer);

            writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading file.

            header.writeRemaining(writer);
            header.close(); // important to close
            var sstInfo = new SSTInfo(file, header, pointerList, filter);
            table.addSST(level, sstInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
