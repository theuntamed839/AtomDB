package search;

import Checksum.Crc32cChecksum;
import Constants.DBConstant;
import Mem.ImmutableMem;
import Mem.ImmutableMemTable;
import Table.SSTInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import db.DBComparator;
import db.KVUnit;
import sst.ValueUnit;
import util.MaxMinAvg;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private final LoadingCache<byte[], ValueUnit> kvCache;
    private final LoadingCache<SSTInfo, Finder> readerCache;
    private final HashMap<Integer, Integer> removeMeAfterTestMap;
    private ImmutableMem<byte[], KVUnit> secondaryMem;
    private final SortedSet<SSTInfo> fileList = new ConcurrentSkipListSet<>();
    MaxMinAvg maker = new MaxMinAvg();

    public Search() {
        this.kvCache = Caffeine.newBuilder()
                .maximumWeight(DBConstant.KEY_VALUE_CACHE_SIZE)
                .weigher((byte[] k, ValueUnit v) -> k.length + v.getSize())
                .build(key -> findKey(key));
        this.readerCache = Caffeine.newBuilder()
                .maximumSize(500)
                .build(sst -> getFinder(sst));
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
        return new Finder(sst.getSst(), sst.getPointers());
    }

    public ValueUnit findKey(byte[] key) throws IOException {
        KVUnit kvUnit = secondaryMem.get(key);
        if (kvUnit != null) {
            return new ValueUnit(kvUnit.getValue(), kvUnit.getIsDelete());
        }
        Crc32cChecksum crc32cChecksum = new Crc32cChecksum();
        long keyChecksum = crc32cChecksum.compute(key);

        int fileRequiredToSearch = 0;

        for (SSTInfo sstInfo : fileList) {
            if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
                fileRequiredToSearch++;

                Finder finder = readerCache.get(sstInfo);
                ValueUnit valueUnit = finder.find(key, keyChecksum);
                if (valueUnit != null) {
                    removeMeAfterTestMap.put(fileRequiredToSearch, removeMeAfterTestMap.getOrDefault(fileRequiredToSearch, 0) + 1);
                    return valueUnit;
                }
            }
        }
        removeMeAfterTestMap.put(fileRequiredToSearch, removeMeAfterTestMap.getOrDefault(fileRequiredToSearch, 0) + 1);
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
        fileList.forEach(each -> System.out.println("Active file="+each.getSst().getName()));
    }
}
