package org.g2n.atomdb.compression;

import net.jpountz.lz4.*;

import java.io.IOException;

public class Lz4Compression implements DataCompressionStrategy {
    private final LZ4CompressorWithLength lz4CompressorWithLength;
    private final LZ4DecompressorWithLength lz4DecompressorWithLength;

    public Lz4Compression() {
        LZ4Factory lz4Factory = LZ4Factory.nativeInstance();
        LZ4Compressor lz4FastCompressor = lz4Factory.fastCompressor();
        this.lz4CompressorWithLength = new LZ4CompressorWithLength(lz4FastCompressor);
        LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();
        this.lz4DecompressorWithLength = new LZ4DecompressorWithLength(decompressor);
    }

    @Override
    public byte[] compress(byte[] arr) throws IOException {
        return lz4CompressorWithLength.compress(arr);
    }

    @Override
    public byte[] decompress(byte[] arr) throws IOException {
        return lz4DecompressorWithLength.decompress(arr);
    }
}
