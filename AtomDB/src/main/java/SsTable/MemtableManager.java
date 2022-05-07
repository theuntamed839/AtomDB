package SsTable;

import Constants.DBConstants;
import com.google.common.primitives.Longs;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemtableManager {
    private ConcurrentSkipListMap<byte[], byte[]> map;
    private Comparator<byte[]> byteArrayComparator = Arrays::compare;
    private int size = 4 * DBConstants.MB;//4098 *4;
    private int total = 0;
    private final static int offsetLength = Longs.toByteArray(1L).length;
    private final SstableManager sstableManager;
    public MemtableManager(int size,SstableManager sstableManager) {
        this.size = size;
        this.sstableManager = sstableManager;
        fresh();
    }

    public MemtableManager(SstableManager sstableManager) {
        this.sstableManager = sstableManager;
        fresh();
    }

    private void fresh() {
        total = 0;
        map = new ConcurrentSkipListMap<>(byteArrayComparator);
    }

    public void put(String key, String value) throws Exception {
        byte[] keyByte = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueByte = value.getBytes(StandardCharsets.UTF_8);
        byte[] put = map.put(keyByte, valueByte);
        if (put != null) {
            System.out.println("already exits key");
        }
        total += keyByte.length + valueByte.length + offsetLength * 2;
        if (total >= size) {
            sstableManager.makeSstable(map);
            fresh();
        }
    }

    public void put(byte[] keyByte, byte[] valueByte) throws Exception {
        byte[] put = map.put(keyByte, valueByte);
        if (put != null) {
            System.out.println("already exits key");
        }
        total += keyByte.length + valueByte.length + offsetLength * 2;
        if (total >= size) {
            sstableManager.makeSstable(map);
            fresh();
        }
    }

    public byte[] get(String key) throws Exception {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (map.get(bytes) != null) {
            System.out.println("got from mem");
            return map.get(bytes);
        }
        return sstableManager.getValue(bytes, key);
    }
}
