package io.github.theuntamed839.datastore4j.compaction;

import io.github.theuntamed839.datastore4j.db.ExpandingByteBuffer;
import io.github.theuntamed839.datastore4j.sstIO.IOReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public record Pointer(byte[] key, long position, Comparator<byte[]> byteArrayComparator) implements Comparable<Pointer> {

    public void storeAsBytes(ExpandingByteBuffer writer) {
        // todo can't we compress the keys ?
        writer.putLong(position)
                .putInt(key.length)
                .put(key);
    }

    public static Pointer getPointer(IOReader reader, Comparator<byte[]> byteArrayComparator) throws IOException {
        long position = reader.getLong();
        int size = reader.getInt();
        var key = new byte[size];
        reader.read(key);
        return new Pointer(key, position, byteArrayComparator);
    }

    public int getSize() {
        return key.length + Long.BYTES;
    }

    @Override
    public int compareTo(Pointer pointer) {
        return byteArrayComparator.compare(this.key, pointer.key);
    }

    @Override
    public String toString() {
        return "Pointer{" +
                "key=" + new String(key, StandardCharsets.UTF_8) +
                ", position=" + position +
                '}';
    }
}
