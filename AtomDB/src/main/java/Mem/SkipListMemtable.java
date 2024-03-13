package Mem;

import Logs.*;
import sst.SSTManager;
import sst.ValueUnit;
import util.SizeOf;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;

public class SkipListMemtable implements Memtable{
    public final Comparator<byte[]> comparator;
    private final File dbFolder;
    private final WALManager wal;
    private ConcurrentSkipListMap<byte[], ValueUnit> memtable;
    private final int sizeLimit = 4 * SizeOf.MB;//4096 *4;
    private int currentSize = 0;
    // todo need to remove
    private SSTManager sstManager;
    public SkipListMemtable(File dbFolder, Comparator<byte[]> comparator, WALManager wal,SSTManager sstManager) throws Exception {
        this.sstManager = sstManager;
        this.dbFolder = dbFolder;
        this.comparator = comparator;
        this.wal = wal;
        fresh();
    }

    private void fresh() {
        memtable = new ConcurrentSkipListMap<>(comparator);
        currentSize = 0;
    }

    private void tryMakingSST() throws Exception {
        if (currentSize >= sizeLimit) {
            sstManager.createSST(Collections.unmodifiableSortedMap(memtable));
            wal.deleteOldLogAndCreateNewLog();
            fresh();
        }
    }

    //todo improve
    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        memtable.put(key, new ValueUnit(value, ValueUnit.ADDED));
        currentSize += key.length + value.length;
        tryMakingSST();
    }

    //todo fix the sstable get
    @Override
    public byte[] get(byte[] key) throws Exception {
        if (memtable.get(key) == null) {
            return sstManager.search(key);
        }
        return memtable.get(key).getValue();
    }

    // todo fix api return type
    @Override
    public byte[] delete(byte[] key) throws Exception {
        // todo could be expensive
        byte[] foundValue = sstManager.search(key);

        if (foundValue == null) {
            ValueUnit value = memtable.remove(key);
            if (value != null && value.getValue() != null) {
                currentSize -= value.getValue().length;
            }
            return null;
        }

        // kv there in sst files,
        // adding a marker
        memtable.put(key, new ValueUnit(ValueUnit.DELETE));

        return null;
    }

    @Override
    public void flush() {
        // todo hard flush
    }
}
