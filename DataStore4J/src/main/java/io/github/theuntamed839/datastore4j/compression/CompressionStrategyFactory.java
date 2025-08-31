package io.github.theuntamed839.datastore4j.compression;

import io.github.theuntamed839.datastore4j.constants.DBConstant;

public class CompressionStrategyFactory {
    public static DataCompressionStrategy getCompressionStrategy(DBConstant.COMPRESSION_TYPE compressionType) {
        return switch (compressionType) {
            case NONE -> new NoCompression();
            case SNAPPY -> new SnappyCompression();
            case LZ4 -> new Lz4Compression();
        };
    }
}
