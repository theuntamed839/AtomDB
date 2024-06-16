package org.example;

public interface BenchmarkDB {
    void put(byte[] key, byte[] value);
    byte[] get(byte[] key);
    void closeAndDestroy();
}
