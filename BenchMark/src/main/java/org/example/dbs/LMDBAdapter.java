package org.example.dbs;

import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteBuffer;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LMDBAdapter implements BenchmarkDBAdapter {
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> db;
    private ByteBuffer keyBuffer;
    private ByteBuffer valBuffer;

    public LMDBAdapter() {
        env = Env.create()
                .setMapSize(1 * 1024 * 1024)   // e.g. 1 GB
                .setMaxDbs(1)
                .open(new File("LMDB" + System.currentTimeMillis()));

        db = env.openDbi("testdb", MDB_CREATE);
        keyBuffer = ByteBuffer.allocateDirect(512);
        valBuffer = ByteBuffer.allocateDirect(512);
    }

    @Override
    public void put(byte[] key, byte[] value) throws Exception {
        keyBuffer.clear();
        valBuffer.clear();
        keyBuffer.put(key).flip();
        valBuffer.put(value).flip();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            db.put(txn, keyBuffer, valBuffer);
            txn.commit();
        }
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        keyBuffer.clear();
        keyBuffer.put(key).flip();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer found = db.get(txn, keyBuffer);
            if (found == null) return null;
            byte[] result = new byte[found.remaining()];
            found.get(result);
            return result;
        }
    }

    @Override
    public void closeAndDestroy() throws Exception {
        db.close();
        env.close();
    }
}

