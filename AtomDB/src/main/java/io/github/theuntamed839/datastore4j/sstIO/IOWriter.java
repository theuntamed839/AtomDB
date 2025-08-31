package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class IOWriter implements AutoCloseable{
    abstract void put(ByteBuffer buffer) throws IOException;
}
