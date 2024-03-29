package db;

public interface DB{

    void put(byte[] key, byte[] value) throws Exception;

    byte[] get(byte[] key) throws Exception;

    void delete(byte[] key) throws Exception;

    void close() throws Exception;

    void destroy();
}
