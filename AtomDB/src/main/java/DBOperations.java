public interface DBOperations {

    void put(byte[] key, byte[] value);

    void get(byte[] key);
}
