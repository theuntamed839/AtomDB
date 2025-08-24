package io.github.theuntamed839.dbs;

public interface BenchmarkDBAdapter {
    void put(byte[] key, byte[] value) throws Exception;
    byte[] get(byte[] key) throws Exception;
    void closeAndDestroy() throws Exception;
}
