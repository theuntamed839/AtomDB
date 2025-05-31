package org.g2n.atomdb.sstIo;

import org.g2n.atomdb.Compaction.IndexedCluster;
import org.g2n.atomdb.Compaction.MergedClusterIterator;
import org.g2n.atomdb.Compaction.Pointer;
import org.g2n.atomdb.Compaction.PointerList;
import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Table.SSTFileNameMeta;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.Table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
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
        Path filePath = this.dbPath.resolve("SST_INTERMEDIATE_" + level + "_1.sst");
        Files.createFile(filePath);

        var inter = writeOptimized1(filePath, level, maxEntries, iterator, () -> true, Integer.MAX_VALUE);
        SSTFileNameMeta meta = table.getNewSST(level);
        Files.move(inter.path(), meta.path(), StandardCopyOption.ATOMIC_MOVE);
        table.addSST(level, new SSTInfo(
                meta.path(),
                inter.sstHeader(),
                inter.pointers(),
                inter.filter(),
                meta
        ));
    }

    public void save(Path path, ExpandingByteBuffer buffer) throws Exception {
        try (IOWriter ioWriter = dbComponentProvider.getIOWriter(path, buffer.remaining())) {
            ioWriter.put(buffer.getBuffer());
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

    public void writeManyFiles(Level level, MergedClusterIterator iterator, int avgNumberOfEntriesInSST) throws Exception {
        SSTInfo sstInfo;
        int count = 0;
        var list = new ArrayList<Intermediate>();
        while (iterator.hasNext()) {
            int finalAvgNumberOfEntriesInSST = avgNumberOfEntriesInSST;
            BooleanSupplier piggyBackingPredicate = () -> finalAvgNumberOfEntriesInSST * 0.10 >= iterator.approximateRemainingEntries();
            Path filePath = this.dbPath.resolve("SST_INTERMEDIATE_" + level + "_" + count++ + ".sst");
            Files.createFile(filePath);
            var intermediate = writeOptimized1(filePath, level, avgNumberOfEntriesInSST, iterator, piggyBackingPredicate, DBConstant.COMPACTED_SST_FILE_SIZE);
            avgNumberOfEntriesInSST = (intermediate.sstHeader().getNumberOfEntries() + avgNumberOfEntriesInSST) / 2;
            list.add(intermediate);
        }

        for (Intermediate intermediate : list) {
            SSTFileNameMeta meta = table.getNewSST(level);
            Files.move(intermediate.path(), meta.path(), StandardCopyOption.ATOMIC_MOVE);
            sstInfo = new SSTInfo(meta.path(), intermediate.sstHeader(), intermediate.pointers(), intermediate.filter(), meta);
            table.addSST(level, sstInfo);
        }
    }

    private Intermediate writeOptimized1(Path filePath, Level level, int avgNumberOfEntriesInSST, Iterator<KVUnit> iterator, BooleanSupplier piggyBackingPredicate, int compactedSstFileSize) throws Exception {
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
        pointers.storeAsBytes(writer);
        writer.putLong(DBConstant.MARK_FILE_END); // todo need confirm this while reading fileToWrite.
        var lastLeftPosition = writer.position();

        // header
        writer.position(0);
        sstHeader.writeSSTHeaderData(writer);
        sstHeader.check();
        writer.position(lastLeftPosition);

        writer.flip();
        save(filePath, writer);
        return new Intermediate(filePath, sstHeader, pointers, filter);
    }
}
