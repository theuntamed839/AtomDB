package io.github.theuntamed839.datastore4j.compression;

import java.io.IOException;

public interface DataCompressionStrategy {
    byte[] compress(byte[] arr) throws IOException;
    byte[] decompress(byte[] arr) throws IOException;
}
