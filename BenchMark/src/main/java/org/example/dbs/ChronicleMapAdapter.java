package org.example.dbs;

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;

public class ChronicleMapAdapter implements BenchmarkDBAdapter {
    private ChronicleMap<byte[], byte[]> map;
    private File file;

    public ChronicleMapAdapter() throws Exception {
        this.file = new File("ChronicleMap_" + System.currentTimeMillis());
        map = ChronicleMap
                .of(byte[].class, byte[].class)
                .name("benchmark")
                .averageKey(new byte[500])
                .averageValue(new byte[50])
                .entries(1_000_000)
                .createPersistedTo(file);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        map.put(key, value);

    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        return map.get(key);
    }

    @Override
    public void closeAndDestroy() throws Exception {
        map.close();
        file.delete();
    }
}
