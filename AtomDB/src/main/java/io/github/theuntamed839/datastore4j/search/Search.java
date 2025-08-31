package io.github.theuntamed839.datastore4j.search;

import io.github.theuntamed839.datastore4j.level.Level;
import io.github.theuntamed839.datastore4j.mem.ImmutableMem;
import io.github.theuntamed839.datastore4j.mem.ImmutableMemTable;
import io.github.theuntamed839.datastore4j.table.SSTInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.db.KVUnit;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class Search implements AutoCloseable{
    private static final ThreadLocal<Checksum> crc32cThreadLocal = ThreadLocal.withInitial(CRC32C::new);
    private final LoadingCache<byte[], KVUnit> kvCache;
    private final SafeCache<SSTInfo, Finder> readerCache;
    private final Map<Integer, Long> readerStats;
    private final DbComponentProvider dbComponentProvider;
    private final SortedSet<SSTInfo> tableView = new TreeSet<>();
    private ImmutableMem<byte[], KVUnit> secondaryMem;
    private final StampedLock lock = new StampedLock();

    public Search(DbComponentProvider dbComponentProvider) {
        this.dbComponentProvider = dbComponentProvider;
        this.kvCache = Caffeine.newBuilder()
                .maximumWeight(dbComponentProvider.getKeyValueCacheSize())
                .weigher((byte[] k, KVUnit v) -> k.length + v.getUnitSize())
                .build(this::findKeyInternal);
        this.readerCache = new SafeCache<>(900, this::getFinder);
        this.secondaryMem = new ImmutableMemTable(new TreeMap<>(dbComponentProvider.getComparator()), 0);
        this.readerStats = new ConcurrentHashMap<>();
    }

    private Finder getFinder(SSTInfo sst) {
        try {
            return new Finder(sst.getPointers(), dbComponentProvider.getIOReader(sst.getSstPath()),
                    sst.getSingleClusterSize(), sst.getCompressionStrategy(), dbComponentProvider.getComparator());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KVUnit findKey(byte[] key) {
        long stamp = lock.readLock();
        try {
            return kvCache.get(key);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private KVUnit findKeyInternal(byte[] key) throws Exception {
        // Note: is locked by the kvCache.
        KVUnit kvUnit = secondaryMem.get(key);
        if (kvUnit != null) {
            return kvUnit;
        }

        long keyChecksum = getKeyChecksum(key);

        int nFileSeekRequired = 0;

        for (SSTInfo sstInfo : tableView) {
            if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
                nFileSeekRequired++;
                Finder finder = readerCache.get(sstInfo);
                try {
                    var unit = finder.find(key, keyChecksum);
                    if (unit != null) {
                        readerStats.merge(nFileSeekRequired, 1L, Long::sum);
                        return unit;
                    }
                } finally {
                    readerCache.release(sstInfo);
                }
            }
        }
        return null;
    }

    public Collection<SSTInfo> getAllSSTsWithKey(byte[] key, Level fromLevel) throws Exception {
        long stamp = lock.readLock();
        try {
            var keyChecksum = getKeyChecksum(key);
            var ssts = new ArrayList<SSTInfo>();
            for (SSTInfo sstInfo : tableView) {
                if (sstInfo.getLevel().isLowerLevelComparedTo(fromLevel)) {
                    continue;
                }
                if (sstInfo.getSstKeyRange().inRange(key) && sstInfo.mightContainElement(key)) {
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
            readerCache.evictAll(toRemove);
            kvCache.invalidateAll();
            kvCache.cleanUp();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlockWrite(writeStamp);
        }
    }

    public Map<Integer, Long> getReaderStats() {
        return Map.copyOf(readerStats);
    }

    private static long getKeyChecksum(byte[] key) {
        Checksum checksum = crc32cThreadLocal.get();
        checksum.reset();
        checksum.update(key);
        return checksum.getValue();
    }

    @Override
    public void close() throws Exception {
        long writeStamp = lock.writeLock();
        try {
            kvCache.invalidateAll();
            kvCache.cleanUp();
            readerCache.close();
        } finally {
            lock.unlockWrite(writeStamp);
        }
    }
}