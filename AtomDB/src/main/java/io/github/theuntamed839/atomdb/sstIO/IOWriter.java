package io.github.theuntamed839.atomdb.sstIO;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class IOWriter implements AutoCloseable{
    abstract void put(ByteBuffer buffer) throws IOException;
}
