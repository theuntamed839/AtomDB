package org.g2n.atomdb.db;

import org.g2n.atomdb.sstIo.*;

import java.io.IOException;
import java.nio.file.Path;

public final class DbComponentProvider {
    IOReaderFactory readerFactory;
    IOWriterFactory writerFactory;

    public DbComponentProvider(DbOptions dbOptions) {
        if (dbOptions.isMMapAllowed()) {
            this.readerFactory = IOMMappedReader::new;
            this.writerFactory = IOMMappedWriter::new;
        } else {
            this.readerFactory = IOFileChannelReader::new;
            this.writerFactory = IOFileChannelWriter::new;
        }
    }

    public IOReader getIOReader(Path file) throws IOException {
        return readerFactory.create(file);
    }

    public IOWriter getIOWriter(Path filePath, long fileSize) throws IOException {
        return writerFactory.create(filePath, fileSize);
    }
}
