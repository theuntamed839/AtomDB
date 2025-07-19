package org.g2n.atomdb.Compression;

public class NoCompression implements DataCompressionStrategy {

    @Override
    public byte[] compress(byte[] arr) {
        return arr;
    }

    @Override
    public byte[] decompress(byte[] arr) {
        return arr;
    }
}
