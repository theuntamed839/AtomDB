package search;

import Checksum.Crc32cChecksum;
import Constants.DBConstant;
import Mem.ImmutableMem;
import Mem.ImmutableMemTable;
import Table.SSTInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import db.KVUnit;
import sst.ValueUnit;

import java.io.IOException;
import java.util.*;

/**
 * we should have a class which will tell us
 * 1. data existence
 * 2. data is of type deleted.
 * 3. data is value
 */
public class Search implements AutoCloseable{

    private final LoadingCache<byte[], ValueUnit> kvCache;
    private final LoadingCache<SSTInfo, Finder> readerCache;
    private final HashMap<Integer, Integer> removeMeAfterTestMap;
    private ImmutableMem<byte[], KVUnit> secondaryMem;
    private final SortedSet<SSTInfo> fileList = new TreeSet<>();

    public Search() {
        this.kvCache = Caffeine.newBuilder()
                .maximumWeight(DBConstant.KEY_VALUE_CACHE_SIZE)
                .weigher((byte[] k, ValueUnit v) -> k.length + v.getSize())
                .build(key -> findKey(key));
        this.readerCache = Caffeine.newBuilder()
                .maximumSize(500)
                .build(sst -> getFinder(sst));
        this.secondaryMem = new ImmutableMemTable(new TreeMap<>(), 0);
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

        List<Finder> list = getFilesToSearch(key);
        removeMeAfterTestMap.put(list.size(), removeMeAfterTestMap.getOrDefault(list.size(), 0) + 1);
        Crc32cChecksum crc32cChecksum = new Crc32cChecksum();
        long keyChecksum = crc32cChecksum.compute(key);
        for (Finder finder : list) {
            ValueUnit valueUnit = finder.find(key, keyChecksum);
            if (valueUnit != null) {
                return valueUnit;
            }
        }
        return null;
    }

    private void log(byte[] key) {
        System.out.println(Arrays.toString(key));
        for (SSTInfo sstInfo : fileList) {
            if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
                System.out.println("sk="+ Arrays.toString(sstInfo.getSstKeyRange().getFirst()) + " lk="+Arrays.toString(sstInfo.getSstKeyRange().getLast()));
            }
        }
    }

    private List<Finder> getFilesToSearch(byte[] key) {
        List<Finder> list = new ArrayList<>();
        for (SSTInfo sstInfo : fileList) {
            if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
                list.add(readerCache.get(sstInfo));
            }
        }
        return list;
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
        kvCache.invalidateAll();
        readerCache.invalidateAll();
        kvCache.cleanUp();
        readerCache.cleanUp();
    }
}
