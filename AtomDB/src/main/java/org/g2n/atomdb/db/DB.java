package org.g2n.atomdb.db;

import java.io.IOException;

public interface DB{

    void put(byte[] key, byte[] value) throws Exception;

    byte[] get(byte[] key) throws Exception;

    void delete(byte[] key) throws Exception;

    void close() throws Exception;

    void destroy() throws IOException;
}
