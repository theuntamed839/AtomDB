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
import org.g2n.atomdb.table.Table;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;

import java.io.IOException;
import java.util.*;

public class Search implements AutoCloseable{

    private final LoadingCache<byte[], KVUnit> kvCache;
    private final LoadingCache<SSTInfo, Finder> readerCache;
    private final Cache<Pointer, Checksums> checksumsCache;
    private final HashMap<Integer, Integer> readerStats;
    private final Table table;
    private final DbComponentProvider dbComponentProvider;
    private final SortedSet<SSTInfo> tableView;
    private ImmutableMem<byte[], KVUnit> secondaryMem;

    public Search(Table table, DbComponentProvider dbComponentProvider) {
        this.table = table;
        this.dbComponentProvider = dbComponentProvider;
        this.kvCache = Caffeine.newBuilder()
                .maximumWeight(DBConstant.KEY_VALUE_CACHE_SIZE)
                .weigher((byte[] k, KVUnit v) -> k.length + v.getUnitSize())
                .build(this::findKey);
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
                .maximumWeight(500 * 1024 * 1024)
                .weigher((Pointer pos, Checksums checks) -> pos.getSize() + checks.getSize())
                .build();
        this.secondaryMem = new ImmutableMemTable(new TreeMap<>(DBComparator.byteArrayComparator), 0);
        this.readerStats = new HashMap<>();
        this.tableView = table.getFileListView();
    }

    public void removeSSTInfo(Collection<SSTInfo> info) {
        readerCache.getAllPresent(info).forEach((key, finder) -> {
            try {
                finder.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        readerCache.invalidateAll(info);
    }

    private Finder getFinder(SSTInfo sst) throws IOException {
        return new Finder(sst.getPointers(), checksumsCache, dbComponentProvider.getIOReader(sst.getSstPath()),
                sst.getSingleClusterSize(), sst.getCompressionStrategy());
    }

    public KVUnit findKey(byte[] key) throws IOException {
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

    private static long getKeyChecksum(byte[] key) {
        Crc32cChecksum crc32cChecksum = Crc32cChecksum.getInstance();//new Crc32cChecksum();
        return crc32cChecksum.compute(key);
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
        kvCache.invalidateAll();
        readerCache.invalidateAll();
        kvCache.cleanUp();
        readerCache.cleanUp();
    }

    public Collection<SSTInfo> getAllSSTsWithKey(byte[] key, Level fromLevel) throws IOException {
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
    }
}