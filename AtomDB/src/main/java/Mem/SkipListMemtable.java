package Mem;

import Constants.Operations;
import Logs.*;
import db.DB;
import db.DBComparator;
import db.DBOptions;
import sst.SSTManager;
import sst.ValueUnit;
import util.BytesConverter;
import util.SizeOf;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListMemtable implements Memtable{
    public final Comparator<byte[]> comparator;
    private final File dbFolder;
    private final WALManager wal;
    private ConcurrentSkipListMap<byte[], ValueUnit> memtable;
    private final int sizeLimit = 4 * SizeOf.MBinBytes;//4096 *4;
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
        if (currentSize > sizeLimit) {
            sstManager.createSST(Collections.unmodifiableSortedMap(memtable));
            fresh();
            wal.deleteOldLogAndCreateNewLog();
        }
    }

    //todo improve
    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        Objects.requireNonNull(key, "null not allowed");
        Objects.requireNonNull(value, "null not allowed");
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
