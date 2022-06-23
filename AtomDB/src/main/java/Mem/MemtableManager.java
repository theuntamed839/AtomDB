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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemtableManager implements Memtable{
    private ConcurrentSkipListMap<byte[], ValueUnit> memtable;
    private final int sizeLimit = 4 * SizeOf.MBinBytes;//4096 *4;
    private int currentSize = 0;
    private LogWriter logWriter;
    private final DBOptions dbOptions;
    // todo need to remove
    private SSTManager sstManager;
    public MemtableManager(DBOptions options, SSTManager sstManager) throws Exception {
        this.dbOptions = options;
        this.sstManager = sstManager;
    }

    public void start(DB db) throws Exception {
        fresh();
        if(!dbOptions.isNew())
            reconstructMem(db);
    }

    // todo
    // recosntruction failed then the whole program shouldn't be stopped
    private void reconstructMem(DB db) throws Exception {
        LogReader logReader = new FileChannelLogReader(dbOptions,
                logWriter.getCurrentFileName());
        logReader.readWAL(db);
    }

    private void fresh() throws IOException {
        memtable = new ConcurrentSkipListMap<>(DBComparator.byteArrayComparator);
        //todo
        // need to created logwriter with new only and use close() to close
        if (logWriter != null )
            logWriter.deleteAndCreateNewLogFile();
        else logWriter = new FileChannelLogWriter(dbOptions);
//        logWriter = new Logs.FileChannelLogWriter(dbOptions);
        currentSize = 0;
    }

    private void tryMakingSST() throws Exception {
        if (currentSize > sizeLimit) {
            System.out.println("creating sst, size= " + memtable.size());
            sstManager.createSST(Collections.unmodifiableSortedMap(memtable));
            fresh();
        }
    }

    //todo improve
    @Override
    public byte[] put(byte[] key, byte[] value) throws Exception {
        Objects.requireNonNull(key, "null not allowed");
        Objects.requireNonNull(value, "null not allowed");

        ValueUnit valueToReturn;
        if (memtable.containsKey(key)) {
            logWriter.logOP(key, value, Operations.UPDATE);
        } else {
            logWriter.logOP(key, value, Operations.WRITE);
        }

        valueToReturn = memtable.put(key, new ValueUnit(value, ValueUnit.ADDED));;
        currentSize += key.length + value.length;
        tryMakingSST();
        return valueToReturn != null ? valueToReturn.getValue() : null;
    }

    //todo fix the sstable get
    @Override
    public byte[] get(byte[] key) throws Exception {
        if (memtable.get(key) == null) {
            return sstManager.search(key);
        }
//        System.out.println("memtable");
        return memtable.get(key).getValue();
    }

    // todo fix api return type
    @Override
    public byte[] delete(byte[] key) throws Exception {
        logWriter.logOP(key,
                BytesConverter.bytes("DELETED_KEY_VALUE"),
                Operations.DELETE);

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
        // todo
    }

    @Override
    public boolean delete(byte[] key, byte[] value) throws Exception {
        // todo
//        sst.ValueUnit foundValue = memtable.get(key);
//        if (foundValue == null || Arrays.compare(foundValue.getValue(), value) != 0) return false;
//        logWriter.logOP(key, bytes, Constants.Operations.DELETE);
//        memtable.remove(key, value);
        return true;
    }

    public void close() throws IOException {
        //debug
//        System.out.println(memtable.entrySet().stream().map(each ->
//                        Map.entry(new String(each.getKey()), new String(each.getValue().getValue())))
//                .toList());
        logWriter.close();
    }
}
