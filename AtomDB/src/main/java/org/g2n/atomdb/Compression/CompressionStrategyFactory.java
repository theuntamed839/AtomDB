package org.g2n.atomdb.Compression;

import org.g2n.atomdb.Constants.DBConstant;

public class CompressionStrategyFactory {
    public static DataCompressionStrategy getCompressionStrategy(DBConstant.COMPRESSION_TYPE compressionType) {
        return switch (compressionType) {
            case NONE -> new NoCompression();
            case SNAPPY -> new SnappyCompression();
            case LZ4 -> new Lz4Compression();
        };
    }
}
