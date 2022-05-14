public interface Memtable {

    void put(byte[] key, byte[] value) throws Exception;

    byte[] get(byte[] key);

    void flush();

    byte[] delete(byte[] key) throws Exception;

    boolean delete(byte[] key, byte[] value) throws Exception;
}
