package org.example;

import java.io.IOException;

public interface BenchmarkDB {
    void put(byte[] key, byte[] value) throws IOException;
    byte[] get(byte[] key) throws IOException;
    void closeAndDestroy() throws IOException;
}
