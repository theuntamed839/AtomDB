package org.g2n.atomdb.SSTIO;

import java.io.File;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

public abstract class ChannelBackedWriter extends OutputStream implements PrimitiveWriter {
    protected File fileToWrite;
    protected FileChannel channel;
}
