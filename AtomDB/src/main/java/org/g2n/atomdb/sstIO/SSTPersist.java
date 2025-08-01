package org.g2n.atomdb.sstIO;

import org.g2n.atomdb.compaction.IndexedCluster;
import org.g2n.atomdb.compaction.MergedClusterIterator;
import org.g2n.atomdb.compaction.Pointer;
import org.g2n.atomdb.compaction.PointerList;
import org.g2n.atomdb.constants.DBConstant;
import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BooleanSupplier;

/*
*
* TODO:
*  1) we can actually keep record the numberOfEntries in sst for that level, which can improve the bloom filter.
*
* */
public class SSTPersist {
    private final Table table;
    private final ThreadLocal<ExpandingByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(ExpandingByteBuffer::new);
    private final Path dbPath;
    private final DbComponentProvider dbComponentProvider;

    public SSTPersist(Table table, Path dbPath, DbComponentProvider dbComponentProvider) {
        this.table = table;
        this.dbPath = dbPath;
        this.dbComponentProvider = dbComponentProvider;
    }

    public void writeSingleFile(Level level, int maxEntries, Iterator<KVUnit> iterator) throws Exception {
        var inter = writeOptimized1(createNewIntermediateSST(level), level, maxEntries, iterator, () -> true, Integer.MAX_VALUE);
        table.addToTheTable(Collections.singletonList(inter));
    }

    public void save(Path path, ExpandingByteBuffer buffer) throws Exception {
        try (IOWriter ioWriter = dbComponentProvider.getIOWriter(path, buffer.remaining())) {
            ioWriter.put(buffer.getBuffer());
        } finally {
            buffer.clear();
        }
    }

    private Path createNewIntermediateSST(Level level) throws IOException {
        Path filePath = dbPath.resolve(level + "_UNFINISHED_SST_" + UUID.randomUUID() + ".sst");
        Files.createFile(filePath);
        return filePath;
    }

    public void writeManyFiles(Level level, MergedClusterIterator iterator, int avgNumberOfEntriesInSST) throws Exception {
        var intermediates = new ArrayList<Intermediate>();
        while (iterator.hasNext()) {
            int finalAvgNumberOfEntriesInSST = avgNumberOfEntriesInSST;
            BooleanSupplier piggyBackingPredicate = () -> finalAvgNumberOfEntriesInSST * 0.10 >= iterator.approximateRemainingEntries();
            var intermediate = writeOptimized1(
                    createNewIntermediateSST(level), level, avgNumberOfEntriesInSST, iterator, piggyBackingPredicate, dbComponentProvider.getSSTSize());
            avgNumberOfEntriesInSST = (intermediate.sstHeader().getNumberOfEntries() + avgNumberOfEntriesInSST) / 2;
            intermediates.add(intermediate);
        }
        table.addToTheTable(intermediates);
    }

    private Intermediate writeOptimized1(Path filePath,
                                         Level level,
                                         int avgNumberOfEntriesInSST,
                                         Iterator<KVUnit> iterator,
                                         BooleanSupplier shouldWePiggyBack,
                                         int compactedSstFileSize) throws Exception {
        var sstHeader = SSTHeader.getDefault(level, dbComponentProvider);
        var writer = bufferThreadLocal.get();
        writer.clear();
        writer.position(SSTHeader.TOTAL_HEADER_SIZE);
        var filter = BloomFilter.create(Funnels.byteArrayFunnel(), avgNumberOfEntriesInSST, 0.01);
        iterator = new ObservingIterator(iterator, filter::put);
        // middle block
        var pointers = new PointerList(avgNumberOfEntriesInSST);
        IndexedCluster indexedCluster = null;
        int totalKVSize = 0;
        int numberOfEntries = 0;
        while (iterator.hasNext() && (totalKVSize < compactedSstFileSize || shouldWePiggyBack.getAsBoolean())) {
            indexedCluster = IndexedCluster.getNextCluster(iterator, sstHeader);
            totalKVSize += indexedCluster.getTotalSize();
            numberOfEntries += indexedCluster.getNumberOfEntries();
            pointers.add(new Pointer(indexedCluster.getFirstKey(), writer.position()));
            indexedCluster.storeAsBytes(writer);
        }

        pointers.add(new Pointer(indexedCluster.getLastKey(), Math.negateExact(writer.position())));
        sstHeader.setEntries(numberOfEntries);
        sstHeader.setFilterPosition(writer.position());

        // footer
        filter.writeTo(writer);
        sstHeader.setPointersPosition(writer.position());
        pointers.storeAsBytes(writer);
        writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading fileToWrite.
        var lastLeftPosition = writer.position();

        // header
        writer.position(0);
        sstHeader.writeSSTHeaderData(writer);
        writer.position(lastLeftPosition);

        writer.flip();
        save(filePath, writer);
        return new Intermediate(filePath, sstHeader, pointers, filter);
    }
}
