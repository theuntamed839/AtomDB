package org.g2n.atomdb.sstIo;

import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public abstract class ChannelBackedReader extends InputStream implements PrimitiveReader {
    protected File file;
    protected RandomAccessFile randomAccessFile;
    protected FileChannel channel;
}