package Compaction;

import sstIo.ChannelBackedWriter;

public record Pointer(byte[] key, long position) {

    public void storeAsBytes(ChannelBackedWriter writer) {
        // todo can't we compress the keys ?
        writer.putLong(position).putInt(key.length).putBytes(key);
    }
}
