public interface DbOperation {

    public boolean put(byte[] key, byte[] value);
    public boolean put(String key, String value);

    public byte[] get(byte[] key);
    public byte[] get(String key);

    public void close();
}
