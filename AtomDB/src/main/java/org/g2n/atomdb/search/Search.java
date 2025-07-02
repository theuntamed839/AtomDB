package org.g2n.atomdb.search;

import org.g2n.atomdb.Checksum.Crc32cChecksum;
import org.g2n.atomdb.Compaction.Pointer;
import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Mem.ImmutableMem;
import org.g2n.atomdb.Mem.ImmutableMemTable;
import org.g2n.atomdb.Table.SSTInfo;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class Search implements AutoCloseable{

    private final LoadingCache<byte[], KVUnit> kvCache;
    private final LoadingCache<SSTInfo, Finder> readerCache;
    private final Cache<Pointer, Checksums> checksumsCache;
    private final HashMap<Integer, Integer> readerStats;
    private final DbComponentProvider dbComponentProvider;
    private ImmutableMem<byte[], KVUnit> secondaryMem;
    private final SortedSet<SSTInfo> fileList = new ConcurrentSkipListSet<>();

    public Search(DbComponentProvider dbComponentProvider) {
        this.dbComponentProvider = dbComponentProvider;
        this.kvCache = Caffeine.newBuilder()
                .maximumWeight(DBConstant.KEY_VALUE_CACHE_SIZE)
                .weigher((byte[] k, KVUnit v) -> k.length + v.getUnitSize())
                .build(this::findKey);
        this.readerCache = Caffeine.newBuilder()
                .maximumSize(900)
                .build(this::getFinder);
        this.checksumsCache = Caffeine.newBuilder()
                .maximumWeight(500 * 1024 * 1024)
                .weigher((Pointer pos, Checksums check) -> DBConstant.CLUSTER_SIZE * Long.BYTES)
                .build();
        this.secondaryMem = new ImmutableMemTable(new TreeMap<>(DBComparator.byteArrayComparator), 0);
        this.readerStats = new HashMap<>();
    }

    public void addSSTInfo(SSTInfo info) {
        fileList.add(info);
    }

    public void removeSSTInfo(SSTInfo info) throws Exception {
        fileList.remove(info);
        Finder exists = readerCache.getIfPresent(info);
        if (exists != null) {
            exists.close();
        }
        readerCache.invalidate(info);
    }

    private Finder getFinder(SSTInfo sst) throws IOException {
        return new Finder(sst.getPointers(), checksumsCache, dbComponentProvider.getIOReader(sst.getSstPath()));
    }

    public KVUnit findKey(byte[] key) throws IOException {
        KVUnit kvUnit = secondaryMem.get(key);
        if (kvUnit != null) {
            return kvUnit;
        }

        Crc32cChecksum crc32cChecksum = Crc32cChecksum.getInstance();//new Crc32cChecksum();
        long keyChecksum = crc32cChecksum.compute(key);

        int fileRequiredToSearch = 0;

        for (SSTInfo sstInfo : fileList) {
            if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
                fileRequiredToSearch++;
                Finder finder = readerCache.get(sstInfo);
                var unit = finder.find(key, keyChecksum);
                if (unit != null) {
                    readerStats.put(fileRequiredToSearch, readerStats.getOrDefault(fileRequiredToSearch, 0) + 1);
                    return unit;
                }
            }
        }
        readerStats.put(fileRequiredToSearch, readerStats.getOrDefault(fileRequiredToSearch, 0) + 1);
        return null;
    }

//    public KVUnit findKey(byte[] key) throws IOException {
//        KVUnit kvUnit = secondaryMem.get(key);
//        if (kvUnit != null) {
//            return kvUnit;
//        }
//
//        Crc32cChecksum crc32cChecksum = Crc32cChecksum.getInstance();//new Crc32cChecksum();
//        long keyChecksum = crc32cChecksum.compute(key);
//
//        int fileRequiredToSearch = 0;
//        for (SSTInfo info : holder.getSSTsContaining(key)) {
//            if (info.mightContainElement(key)) {
//                fileRequiredToSearch++;
//                Finder finder = readerCache.get(info);
//                var unit = finder.find(key, keyChecksum);
//                if (unit != null) {
//                    readerStats.put(fileRequiredToSearch, readerStats.getOrDefault(fileRequiredToSearch, 0) + 1);
//                    return unit;
//                }
//            }
//        }
//        readerStats.put(fileRequiredToSearch, readerStats.getOrDefault(fileRequiredToSearch, 0) + 1);
//        return null;
//    }

    public void addSecondaryMemtable(ImmutableMem<byte[], KVUnit> mem) {
        this.secondaryMem = mem;
        for (Iterator<KVUnit> it = mem.getKeySetIterator(); it.hasNext(); ) {
            byte[] key = it.next().getKey();
            kvCache.invalidate(key);
        }
    }


    @Override
    public void close() throws Exception {
        for (Map.Entry<Integer, Integer> entry : readerStats.entrySet()) {
            System.out.println("numberOfFilesRequiredToSearch="+entry.getKey()+" numberOfTimesThisHappened="+entry.getValue());
        }
        for (Finder value : readerCache.asMap().values()) {
            value.close();
        }
        kvCache.invalidateAll();
        readerCache.invalidateAll();
        kvCache.cleanUp();
        readerCache.cleanUp();
    }
}