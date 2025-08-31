package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.io.InputStream;

public abstract class IOReader extends InputStream {
    // input stream
    @Override
    public abstract int read() throws IOException;

    @Override
    public abstract int read(byte[] b) throws IOException;

    @Override
    public abstract int read(byte[] b, int off, int len) throws IOException;

    public abstract long getLong() throws IOException;

    public abstract int getInt() throws IOException;

    public abstract long position() throws IOException;

    public abstract void position(long positionToMove) throws IOException;

    public abstract boolean hasRemaining() throws IOException;

    public abstract byte get() throws IOException;

    public abstract void get(byte[] k) throws IOException;

    @Override
    public abstract void close() throws IOException;
}
