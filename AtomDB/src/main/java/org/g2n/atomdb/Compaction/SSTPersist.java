package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.Table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.SSTHeader;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.function.BooleanSupplier;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/*
*
* TODO:
*  1) we can actually keep record the numberOfEntries in sst for that level, which can improve the bloom filter.
*
* */
public class SSTPersist {
    private final Table table;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);

    public SSTPersist(Table table) {
        this.table = table;
    }

    public void writeSingleFile(Level level, int maxEntries, Iterator<KVUnit> iterator) throws IOException {
        var sstInfo = writeOptimized1(table.getNewSST(level), level, maxEntries, iterator, () -> true, Integer.MAX_VALUE);
        table.addSST(level, sstInfo);
        System.out.println(sstInfo);
    }

    public void save(File file) throws IOException {
        var buffer = bufferThreadLocal.get();
        buffer.flip();
        try (
                var fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
                var arena = Arena.ofConfined()
        ) {
            var fileSegment = fileChannel.map(READ_WRITE, 0, buffer.remaining(), arena);
            fileSegment.asByteBuffer().put(buffer.getBuffer());
        } finally {
            buffer.clear();
        }
    }

    private IndexedCluster getNextCluster(Iterator<KVUnit> customIterator, BloomFilter<byte[]> filter) {
        var cluster = new IndexedCluster(DBConstant.CLUSTER_SIZE);
        for (int i = 0; i < DBConstant.CLUSTER_SIZE && customIterator.hasNext(); i++) {
            KVUnit current = customIterator.next();
            cluster.add(current);
            filter.put(current.getKey());
        }
        return cluster;
    }

    public void writeManyFiles(Level level, MergedClusterIterator iterator, int avgNumberOfEntriesInSST) throws IOException {
        SSTInfo sstInfo = null;

        while (iterator.hasNext()) {
            int finalAvgNumberOfEntriesInSST = avgNumberOfEntriesInSST;
            BooleanSupplier piggyBackingPredicate = () -> finalAvgNumberOfEntriesInSST * 0.10 >= iterator.approximateRemainingEntries();

            sstInfo = writeOptimized1(table.getNewSST(level), level, avgNumberOfEntriesInSST, iterator, piggyBackingPredicate, DBConstant.COMPACTED_SST_FILE_SIZE);
            avgNumberOfEntriesInSST = (sstInfo.getNumberOfEntries() + avgNumberOfEntriesInSST) / 2;
            Validator.validateSST(sstInfo);
            table.addSST(level, sstInfo);
            System.out.println(sstInfo);
        }
        table.saveLastCompactedKey(level, sstInfo.getSstKeyRange().getGreatest());
    }

    private SSTInfo writeOptimized1(File file, Level level, int avgNumberOfEntriesInSST, Iterator<KVUnit> iterator, BooleanSupplier piggyBackingPredicate, int compactedSstFileSize) throws IOException {
        System.out.println("new compaction file name ="+ file.getName());
        var sstHeader = SSTHeader.getDefault(level);
        var writer = bufferThreadLocal.get();
        writer.clear();
        writer.position(SSTHeader.TOTAL_HEADER_SIZE);
        var filter = BloomFilter.create(Funnels.byteArrayFunnel(), avgNumberOfEntriesInSST, 0.01);

        // middle block
        var pointers = new PointerList(avgNumberOfEntriesInSST);
        IndexedCluster indexedCluster = null;
        int totalKVSize = 0;
        int numberOfEntries = 0;
        while (iterator.hasNext() && (totalKVSize < compactedSstFileSize || piggyBackingPredicate.getAsBoolean())) {
            indexedCluster = getNextCluster(iterator, filter);
            totalKVSize += indexedCluster.getTotalSize();
            numberOfEntries += indexedCluster.getNumberOfEntries();
            pointers.add(new Pointer(indexedCluster.getFirstKey(), writer.position()));
//            System.out.println("pointer position"+ writer.position());
            indexedCluster.storeAsBytes(writer);
        }

        pointers.add(new Pointer(indexedCluster.getLastKey(), Math.negateExact(writer.position())));
        sstHeader.setEntries(numberOfEntries);
        sstHeader.setFilterPosition(writer.position());

        // footer
        filter.writeTo(writer);
        sstHeader.setPointersPosition(writer.position());
        System.out.println("pointer possition"+ writer.position());
        pointers.storeAsBytes(writer);
        writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading fileToWrite.
        var lastLeftPosition = writer.position();

        // header
        writer.position(0);
        sstHeader.writeSSTHeaderData(writer);
        sstHeader.check();
        writer.position(lastLeftPosition);

        save(file);
        return new SSTInfo(file, sstHeader, pointers, filter);
    }
}
