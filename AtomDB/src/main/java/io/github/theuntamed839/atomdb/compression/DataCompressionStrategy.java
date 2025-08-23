package io.github.theuntamed839.atomdb.compression;

import java.io.IOException;

public interface DataCompressionStrategy {
    byte[] compress(byte[] arr) throws IOException;
    byte[] decompress(byte[] arr) throws IOException;
}
