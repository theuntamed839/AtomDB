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
import org.g2n.atomdb.util.MaxMinAvg;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * we should have a class which will tell us
 * 1. data existence
 * 2. data is of type deleted.
 * 3. data is value
 * 1beafb0b371cf3b51732a84d4d82acd7b4926ba1 commit is faster.
 * https://github.com/theuntamed839/AtomDB/commit/1beafb0b371cf3b51732a84d4d82acd7b4926ba1
 */

public class Search implements AutoCloseable{

    private final LoadingCache<byte[], KVUnit> kvCache;
    private final LoadingCache<SSTInfo, Finder> readerCache;
    private final Cache<Pointer, Checksums> checksumsCache;
    private final HashMap<Integer, Integer> removeMeAfterTestMap;
    private final DbComponentProvider dbComponentProvider;
    private ImmutableMem<byte[], KVUnit> secondaryMem;
    private final SortedSet<SSTInfo> fileList = new ConcurrentSkipListSet<>();
    MaxMinAvg maker = new MaxMinAvg();

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
        this.removeMeAfterTestMap = new HashMap<>();
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
        Crc32cChecksum crc32cChecksum = new Crc32cChecksum();
        long keyChecksum = crc32cChecksum.compute(key);

        int fileRequiredToSearch = 0;

        for (SSTInfo sstInfo : fileList) {
            if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
                fileRequiredToSearch++;
//                System.out.println("trying sstInfo="+sstInfo);
//                Validator.validateSSTBasedOnSearch(sstInfo, key);
                Finder finder = readerCache.get(sstInfo);
                var unit = finder.find(key, keyChecksum);
                if (unit != null) {
                    removeMeAfterTestMap.put(fileRequiredToSearch, removeMeAfterTestMap.getOrDefault(fileRequiredToSearch, 0) + 1);
                    return unit;
                }
            }
        }
        removeMeAfterTestMap.put(fileRequiredToSearch, removeMeAfterTestMap.getOrDefault(fileRequiredToSearch, 0) + 1);
//        System.out.println(this.getClass().getName() + " :Key not found in any file");
//        fileList.forEach(each -> {
//            try {
//                Validator.validateSSTBasedOnSearch(each, key);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
        return null;
    }

    public void addSecondaryMemtable(ImmutableMem<byte[], KVUnit> mem) {
        this.secondaryMem = mem;
        for (Iterator<KVUnit> it = mem.getKeySetIterator(); it.hasNext(); ) {
            byte[] key = it.next().getKey();
            kvCache.invalidate(key);
        }
    }


    @Override
    public void close() throws Exception {
        for (Map.Entry<Integer, Integer> entry : removeMeAfterTestMap.entrySet()) {
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

    public void printActiveFiles() {
        fileList.forEach(each -> System.out.println("Active fileToWrite="+each.getSstPath()));
    }
}
