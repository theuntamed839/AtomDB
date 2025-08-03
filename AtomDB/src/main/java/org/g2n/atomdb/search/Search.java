package org.g2n.atomdb.search;

import org.g2n.atomdb.checksum.Crc32cChecksum;
import org.g2n.atomdb.compaction.Pointer;
import org.g2n.atomdb.constants.DBConstant;
import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.mem.ImmutableMem;
import org.g2n.atomdb.mem.ImmutableMemTable;
import org.g2n.atomdb.table.SSTInfo;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.StampedLock;

public class Search implements AutoCloseable{

    private final LoadingCache<byte[], KVUnit> kvCache;
    private final LoadingCache<SSTInfo, Finder> readerCache;
    private final Cache<Pointer, Checksums> checksumsCache;
    private final HashMap<Integer, Integer> readerStats;
    private final DbComponentProvider dbComponentProvider;
    private final SortedSet<SSTInfo> tableView = new TreeSet<>();
    private ImmutableMem<byte[], KVUnit> secondaryMem;
    private final StampedLock lock = new StampedLock();

    public Search(DbComponentProvider dbComponentProvider) {
        this.dbComponentProvider = dbComponentProvider;
        this.kvCache = Caffeine.newBuilder()
                .maximumWeight(DBConstant.KEY_VALUE_CACHE_SIZE)
                .weigher((byte[] k, KVUnit v) -> k.length + v.getUnitSize())
                .build(this::findKeyInternal);
        this.readerCache = Caffeine.newBuilder()
                .maximumSize(900)
                .removalListener((key, value, cause) -> {
                    try {
                        ((AutoCloseable)value).close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .build(this::getFinder);
        this.checksumsCache = Caffeine.newBuilder()
                .maximumWeight(100 * DBConstant.MB)
                .weigher((Pointer pos, Checksums checks) -> pos.getSize() + checks.getSize())
                .build();
        this.secondaryMem = new ImmutableMemTable(new TreeMap<>(DBComparator.byteArrayComparator), 0);
        this.readerStats = new HashMap<>();
    }

    private Finder getFinder(SSTInfo sst) throws IOException {
        return new Finder(sst.getPointers(), checksumsCache, dbComponentProvider.getIOReader(sst.getSstPath()),
                sst.getSingleClusterSize(), sst.getCompressionStrategy());
    }

    public KVUnit findKey(byte[] key) throws IOException {
        long stamp = lock.readLock();
        try {
//            return kvCache.get(key);
            return findKeyInternal(key);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private KVUnit findKeyInternal(byte[] key) throws IOException {
        KVUnit kvUnit = secondaryMem.get(key);
        if (kvUnit != null) {
            return kvUnit;
        }

        long keyChecksum = getKeyChecksum(key);

        int fileRequiredToSearch = 0;

        for (SSTInfo sstInfo : tableView) {
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

    public Collection<SSTInfo> getAllSSTsWithKey(byte[] key, Level fromLevel) throws IOException {
        long stamp = lock.readLock();
        try {
            var keyChecksum = getKeyChecksum(key);
            var ssts = new ArrayList<SSTInfo>();
            for (SSTInfo sstInfo : tableView) {
                if (sstInfo.getLevel().compareTo(fromLevel) < 0) {
                    continue;
                }
                if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) { // todo can settle for only mightContainElement check ?
                    Finder finder = readerCache.get(sstInfo);
                    var unit = finder.find(key, keyChecksum);
                    if (unit != null) {
                        ssts.add(sstInfo);
                    }
                }
            }
            return ssts;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void addSecondaryMemtable(ImmutableMem<byte[], KVUnit> mem) {
        long stamp = lock.writeLock();
        try {
            if (secondaryMem == null) {
                throw new IllegalStateException("Secondary memtable can't be null");
            }
            this.secondaryMem = mem;
            kvCache.invalidateAll(mem.getkeySet()); // since these latest kv, we want to discard any previous copies of these keys in the cache
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void addAndRemoveSST(Collection<SSTInfo> added, Collection<SSTInfo> toRemove) {
        long writeStamp = lock.writeLock();
        try {
            tableView.addAll(added);
            tableView.removeAll(toRemove);
            readerCache.invalidateAll(toRemove);
            kvCache.invalidateAll();
            kvCache.cleanUp();
        } finally {
            lock.unlockWrite(writeStamp);
        }
    }

    private static long getKeyChecksum(byte[] key) {
        Crc32cChecksum crc32cChecksum = Crc32cChecksum.getInstance();//new Crc32cChecksum();
        return crc32cChecksum.compute(key);
    }

    @Override
    public void close() throws Exception {
        long totalReads = readerStats.values().stream().mapToInt(Integer::intValue).sum();
        for (Map.Entry<Integer, Integer> entry : readerStats.entrySet()) {
            System.out.println("numberOfFilesRequiredToSearch="+entry.getKey()+" numberOfTimesThisHappened= "+(entry.getValue() * 100.0/totalReads) + "%");
        }
        kvCache.invalidateAll();
        readerCache.invalidateAll();
        kvCache.cleanUp();
        readerCache.cleanUp();
    }
}