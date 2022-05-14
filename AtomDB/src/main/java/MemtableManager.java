import com.google.common.primitives.Longs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemtableManager implements Memtable{
    private ConcurrentSkipListMap<byte[], byte[]> memtable;
    private final int sizeLimit = 4 * SizeOf.MBinBytes;//4098 *4;
    private int currentSize = 0;
    private final static int offsetLength = Longs.toByteArray(1L).length;
//    private final SstableManager sstableManager;
    private LogWriter logWriter;
    private DBOptions dbOptions;
    public MemtableManager(DBOptions options) throws IOException {
        dbOptions = options;
        new File(options.getDBfolder()).mkdir(); // this needs to go in dbimpl
        fresh();
    }

    private void fresh() throws IOException {
        memtable = new ConcurrentSkipListMap<>(DBComparator.byteArrayComparator);
        logWriter = new FileChannelLogWriter(dbOptions);
        currentSize = 0;
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        if (memtable.containsKey(key)) {
            logWriter.logOP(key, value, Operations.UPDATE);
            memtable.put(key, value);
        } else {
            logWriter.logOP(key, value, Operations.WRITE);
            memtable.put(key, value);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return memtable.get(key);
    }

    @Override
    public void flush() {

    }

    @Override
    public byte[] delete(byte[] key) throws Exception {
        if (!memtable.containsKey(key)) throw new Exception("no such key added");

        logWriter.logOP(key, memtable.get(key), Operations.DELETE);
        return memtable.remove(key);
    }

    @Override
    public boolean delete(byte[] key, byte[] value) throws Exception {
        byte[] bytes = memtable.get(key);
        if (bytes == null || Arrays.compare(bytes, value) != 0) return false;
        logWriter.logOP(key, bytes, Operations.DELETE);
        memtable.remove(key, value);
        return true;
    }

    public void close() throws IOException {
        //debug
        System.out.println(memtable.entrySet().stream().map(each ->
                        Map.entry(new String(each.getKey()), new String(each.getValue())))
                .toList());
        logWriter.close();
    }
}
