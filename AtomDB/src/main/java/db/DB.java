package db;

import java.io.IOException;

public interface DB{

    byte[] put(byte[] key, byte[] value) throws Exception;

    byte[] get(byte[] key) throws Exception;

    byte[] delete(byte[] key) throws Exception;

    void close() throws IOException;

    void destroy();
}
