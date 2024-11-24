package Compaction;

import Constants.DBConstant;
import Level.Level;
import Table.SSTInfo;
import Table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.KVUnit;
import sstIo.FullFileBufferedWriter;
import sstIo.SSTHeader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class SSTPersist {
    private static SSTHeader createHeader(Level level) {
        return new SSTHeader(
                DBConstant.SST_VERSION,
                level,
                DBConstant.CRC32C_CHECKSUM_TYPE,
                DBConstant.LZ4_COMPRESSION_TYPE,
                DBConstant.CLUSTER_SIZE,
                DBConstant.SHORTEST_COMMON_PREFIX_USED);
    }

    public static void writeManyFiles(Level level, CollectiveSStIterator iterator, Table table) throws Exception {
        SSTInfo last = null;
        while(iterator.hasNext()) {
            last = writeOptimized(table.getNewSST(level), level, 100000, iterator, DBConstant.COMPACTED_SST_FILE_SIZE);
            table.addSST(level, last);
        }
        iterator.close();
        table.saveLastCompactedKey(last.getSstKeyRange().getGreatest(), level);
    }

    public static SSTInfo writeSingleFile(Level level, int maxEntries, Iterator<KVUnit> iterator, Table table) throws IOException {
        var sstInfo = writeOptimized(table.getNewSST(level), level, maxEntries, iterator, Integer.MAX_VALUE);
        table.addSST(level, sstInfo);
        return sstInfo;
    }

    public static SSTInfo writeOptimized(File file, Level level, int upperLimitOfEntries, Iterator<KVUnit> iterator, int SST_SIZE) {
        try
        {
            var writer = new FullFileBufferedWriter(file);
            BloomFilter<byte[]> filter = BloomFilter.create(
                    Funnels.byteArrayFunnel(),
                    upperLimitOfEntries,
                    0.01);
            FilterAddingIterator customIterator = new FilterAddingIterator(iterator, filter);
            var pointers = new PointerList(upperLimitOfEntries);

            var sstHeader = createHeader(level);
            sstHeader.storeAsBytes(writer);
            // middle block
            IndexedCluster indexedCluster = null;
            while(customIterator.hasNext() && customIterator.getTotalKVSize() < SST_SIZE) {
                indexedCluster = getNextCluster(customIterator);
                pointers.add(new Pointer(indexedCluster.getFirstKey(), writer.position()));
                indexedCluster.storeAsBytes(writer);
            }
            pointers.add(new Pointer(indexedCluster.getLastKey(), Math.negateExact(writer.position())));
            // footer
            sstHeader.setEntries(customIterator.getCount());
            sstHeader.setFilterPosition((int) writer.position());
            filter.writeTo(writer);

            sstHeader.setPointersPosition((int) writer.position());
            pointers.storeAsBytes(writer);

            writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading file.
            var lastLeftPosition = writer.position();
            sstHeader.writeRemaining(writer);
            sstHeader.close(); // important to close
            writer.position(lastLeftPosition);
            writer.close();
            var sstInfo = new SSTInfo(file, sstHeader, pointers, filter);
            return sstInfo;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexedCluster getNextCluster(Iterator<KVUnit> customIterator) {
        var cluster = new IndexedCluster(DBConstant.CLUSTER_SIZE);
        for (int i = 0; i < DBConstant.CLUSTER_SIZE && customIterator.hasNext(); i++) {
            KVUnit current = customIterator.next();
            cluster.add(current);
        }
        return cluster;
    }
}
