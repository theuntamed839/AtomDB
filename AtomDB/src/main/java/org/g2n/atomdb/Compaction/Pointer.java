package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.sstIo.ChannelBackedWriter;
import org.g2n.atomdb.sstIo.IOReader;

import java.nio.MappedByteBuffer;

// todo make the position int.
public record Pointer(byte[] key, long position) implements Comparable<Pointer> {

    public void storeAsBytes(ChannelBackedWriter writer) {
        // todo can't we compress the keys ?
        writer.putLong(position)
                .putInt(key.length)
                .putBytes(key);
    }

    public void storeAsBytes(ExpandingByteBuffer writer) {
        // todo can't we compress the keys ?
        writer.putLong(position)
                .putInt(key.length)
                .put(key);
    }

    public static Pointer getPointer(IOReader reader) {
        long position = reader.getLong();
        int size = reader.getInt();
        var key = new byte[size];
        reader.getBytes(key);
        return new Pointer(key, position);
    }

    public static Pointer readBytesToObj(MappedByteBuffer buffer) {
        long pos = buffer.getLong();
        var key = new byte[buffer.getInt()];
        buffer.get(key);
        return new Pointer(key, pos);
    }
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        Pointer pointer = (Pointer) o;
//        return position == pointer.position && Arrays.equals(key, pointer.key);
//    }
//
//    @Override
//    public int hashCode() {
//        int result = Objects.hash(position);
//        result = 31 * result + Arrays.hashCode(key);
//        return result;
//    }


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
