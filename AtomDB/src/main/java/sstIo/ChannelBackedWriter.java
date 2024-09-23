package sstIo;

import java.io.File;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

public abstract class ChannelBackedWriter extends OutputStream implements PrimitiveWriter {
    protected File file;
    protected RandomAccessFile randomAccessFile;
    protected FileChannel channel;
}
