public class DbImpl implements DbOperation{

    @Override
    public boolean put(byte[] key, byte[] value) {
        return false;
    }

    @Override
    public boolean put(String key, String value) {
        return false;
    }

    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public byte[] get(String key) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
