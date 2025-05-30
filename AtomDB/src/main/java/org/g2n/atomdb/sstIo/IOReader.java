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

    public abstract long getLong();

    public abstract int getInt();

    public abstract byte getByte();

    public abstract int getBytes(byte[] item);

    public abstract int getBytes(byte[] item, int offset, int length);

    public abstract long position();

    public abstract void position(int positionToMove);

    @Override
    public abstract void close() throws IOException;

    public abstract boolean stillAvailable();

    public abstract byte get();

    public abstract void get(byte[] k);
}
