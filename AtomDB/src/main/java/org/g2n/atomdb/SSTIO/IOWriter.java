package org.g2n.atomdb.SSTIO;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class IOWriter implements AutoCloseable{
    abstract void put(ByteBuffer buffer) throws IOException;
}
