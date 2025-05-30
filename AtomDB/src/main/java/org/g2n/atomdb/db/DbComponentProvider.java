package org.g2n.atomdb.db;

import org.g2n.atomdb.sstIo.FileChannelBackedReader;
import org.g2n.atomdb.sstIo.IOReader;
import org.g2n.atomdb.sstIo.IOReaderFactory;
import org.g2n.atomdb.sstIo.MMappedBackedReader;

import java.io.IOException;
import java.nio.file.Path;

public final class DbComponentProvider {
    IOReaderFactory readerFactory;

    public DbComponentProvider(DbOptions dbOptions) {
        this.readerFactory = dbOptions.isMMapAllowed() ? MMappedBackedReader::new : FileChannelBackedReader::new;
    }

    public IOReader getIOReader(Path file) throws IOException {
        return readerFactory.create(file);
    }

}
