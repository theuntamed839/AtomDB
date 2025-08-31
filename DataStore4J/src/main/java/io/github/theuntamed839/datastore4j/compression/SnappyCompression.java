package io.github.theuntamed839.datastore4j.compression;

import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyCompression implements DataCompressionStrategy {

    @Override
    public byte[] compress(byte[] arr) throws IOException {
        return Snappy.compress(arr);
    }

    @Override
    public byte[] decompress(byte[] arr) throws IOException {
        return Snappy.uncompress(arr);
    }
}
