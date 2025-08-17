package org.example.dbs;

import org.mapdb.*;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

public class MapDBAdapter implements BenchmarkDBAdapter {
    private final File file;
    private DB db;
    private ConcurrentMap<byte[], byte[]> map;

    public MapDBAdapter() {
        this.file = new File("MapDB_" + System.currentTimeMillis());

        db = DBMaker.fileDB(file).make();
        map = db.hashMap("map", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
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
        db.close();
        file.delete();
    }
}

