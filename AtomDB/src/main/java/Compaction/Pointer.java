package Compaction;

import sstIo.ChannelBackedWriter;

import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public record Pointer(byte[] key, long position) {

    public void storeAsBytes(ChannelBackedWriter writer) {
        // todo can't we compress the keys ?
        writer.putLong(position)
                .putInt(key.length)
                .putBytes(key);
    }

    public static Pointer readBytesToObj(MappedByteBuffer buffer) {
        long pos = buffer.getLong();
        var key = new byte[buffer.getInt()];
        buffer.get(key);
        return new Pointer(key, pos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pointer pointer = (Pointer) o;
        return position == pointer.position && Arrays.equals(key, pointer.key);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(position);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
