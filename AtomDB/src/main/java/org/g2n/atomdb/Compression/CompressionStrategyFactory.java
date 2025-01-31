package org.g2n.atomdb.Compression;

public class CompressionStrategyFactory {
    public static DataCompressionStrategy GetCompressionStrategy(boolean compressionDisabled)
    {
        if (compressionDisabled) {
            return new NoCompression();
        }
        return new SnappyCompression();
    }
}
