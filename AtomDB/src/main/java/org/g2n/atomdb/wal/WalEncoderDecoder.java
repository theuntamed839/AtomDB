package org.g2n.atomdb.wal;

import org.g2n.atomdb.constants.Operations;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIO.IOReader;

import java.io.IOException;

public final class WalEncoderDecoder {

    public void encode(ExpandingByteBuffer buffer, Operations operations, KVUnit kvUnit) {
        buffer.put(operations.value());
        byte[] key = kvUnit.getKey();
        byte[] value = kvUnit.getValue();
        buffer.putInt(Integer.BYTES + key.length + KVUnit.TOMBSTONE_BYTES + (value != null ? Integer.BYTES + value.length : 0)) // setting total length to read
                .putInt(key.length)
                .put(key)
                .put(kvUnit.getTombStoneValue());
        if (!kvUnit.isTombStone()) {
            buffer.putInt(value.length).put(value);
        }
    }

    public LogBlock decoder(IOReader reader) throws IOException {
        var operation = Operations.getOperation(reader.get());
        int totalKvLength = reader.getInt();
        int keyLength = reader.getInt();
        var key = new byte[keyLength];
        reader.get(key);
        var marker = reader.get();
        if (!KVUnit.isTombStone(marker)) {
            int valueLength = reader.getInt();
            var value = new byte[valueLength];
            reader.get(value);
            return new LogBlock(operation, new KVUnit(key, value));
        }
        return new LogBlock(operation, new KVUnit(key));
    }
}
