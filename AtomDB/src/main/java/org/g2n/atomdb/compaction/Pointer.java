package org.g2n.atomdb.compaction;

import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.sstIO.IOReader;

import java.io.IOException;

// todo make the position int.
public record Pointer(byte[] key, long position) implements Comparable<Pointer> {

    public void storeAsBytes(ExpandingByteBuffer writer) {
        // todo can't we compress the keys ?
        writer.putLong(position)
                .putInt(key.length)
                .put(key);
    }

    public static Pointer getPointer(IOReader reader) throws IOException {
        long position = reader.getLong();
        int size = reader.getInt();
        var key = new byte[size];
        reader.read(key);
        return new Pointer(key, position);
    }

    public int getSize() {
        return key.length + Long.BYTES;
    }

    @Override
    public int compareTo(Pointer pointer) {
        return DBComparator.byteArrayComparator.compare(this.key, pointer.key);
    }

    @Override
    public String toString() {
        return "Pointer{" +
                "key=" + new String(key) +
                ", position=" + position +
                '}';
    }
}
