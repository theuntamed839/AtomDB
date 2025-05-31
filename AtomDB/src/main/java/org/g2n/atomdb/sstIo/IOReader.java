package org.g2n.atomdb.sstIo;

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

    public abstract byte getByte() throws IOException;

    public abstract int getBytes(byte[] item) throws IOException;

    public abstract int getBytes(byte[] item, int offset, int length) throws IOException;

    public abstract long position() throws IOException;

    public abstract void position(int positionToMove) throws IOException;

    @Override
    public abstract void close() throws IOException;

    public abstract boolean stillAvailable() throws IOException;

    public abstract byte get() throws IOException;

    public abstract void get(byte[] k) throws IOException;
}
