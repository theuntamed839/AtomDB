package Compression;

import java.io.IOException;

public interface DataCompressionStrategy {
    public byte[] compress(byte[] arr) throws IOException;
    public byte[] decompress(byte[] arr) throws IOException;
}
