package Mem;

public interface Memtable {

    byte[] put(byte[] key, byte[] value) throws Exception;

    byte[] get(byte[] key) throws Exception;

    void flush();

    byte[] delete(byte[] key) throws Exception;

    boolean delete(byte[] key, byte[] value) throws Exception;
}
