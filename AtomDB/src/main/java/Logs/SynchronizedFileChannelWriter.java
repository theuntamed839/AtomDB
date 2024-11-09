package Logs;

import db.ExpandingByteBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class SynchronizedFileChannelWriter implements Writer, SynchronizedWriter {
    private final RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private final File file;

    public SynchronizedFileChannelWriter(File file) throws IOException {
        this.file = file;
        randomAccessFile = new RandomAccessFile(file, "rw");
        channel = randomAccessFile.getChannel();
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
        randomAccessFile.close();
    }

    @Override
    public synchronized void write(byte[] buffer) throws IOException {
        write(ByteBuffer.wrap(buffer));
    }

    @Override
    public synchronized void write(ByteBuffer buffer) throws IOException {
        channel.write(buffer);
    }
}
