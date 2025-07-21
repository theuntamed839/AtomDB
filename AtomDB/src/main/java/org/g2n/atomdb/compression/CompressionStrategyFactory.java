package org.g2n.atomdb.compression;

import org.g2n.atomdb.constants.DBConstant;

public class CompressionStrategyFactory {
    public static DataCompressionStrategy getCompressionStrategy(DBConstant.COMPRESSION_TYPE compressionType) {
        return switch (compressionType) {
            case NONE -> new NoCompression();
            case SNAPPY -> new SnappyCompression();
            case LZ4 -> new Lz4Compression();
        };
    }
}
