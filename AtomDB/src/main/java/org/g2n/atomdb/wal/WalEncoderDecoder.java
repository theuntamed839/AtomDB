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
        var isDeleted = kvUnit.getDeletedStatus();
        byte[] value = kvUnit.getValue();
        buffer.putInt(Integer.BYTES + key.length + KVUnit.DeletionStatus.BYTES + (value != null ? Integer.BYTES + value.length : 0)) // setting total length to read
                .putInt(key.length)
                .put(key)
                .put(isDeleted.value());
        if (!kvUnit.isDeleted()) {
            buffer.putInt(value.length).put(value);
        }
    }

    public LogBlock decoder(IOReader reader) throws IOException {
        var operation = Operations.getOperation(reader.get());
        int totalKvLength = reader.getInt();
        int kLenght = reader.getInt();
        var k = new byte[kLenght];
        reader.get(k);
        var isDeleted = KVUnit.DeletionStatus.of(reader.get());
        if (KVUnit.DeletionStatus.NOT_DELETED == isDeleted) {
            int vLenght = reader.getInt();
            var v = new byte[vLenght];
            reader.get(v);
            return new LogBlock(operation, new KVUnit(k, v));
        }
        return new LogBlock(operation, new KVUnit(k));
    }
}
