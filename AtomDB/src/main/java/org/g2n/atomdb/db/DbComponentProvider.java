package org.g2n.atomdb.db;

import org.g2n.atomdb.constants.DBConstant;
import org.g2n.atomdb.sstIO.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

public final class DbComponentProvider {
    private final byte clusterSize;
    private final IOReaderFactory readerFactory;
    private final IOWriterFactory writerFactory;
    private final DBConstant.CHECKSUM_TYPE checksumType;
    private final DBConstant.COMPRESSION_TYPE compressionType;
    private final int sstFileSize;
    private final Comparator<byte[]> comparator;

    public DbComponentProvider(DbOptions dbOptions) {
        if (dbOptions.isMMapAllowed()) {
            this.readerFactory = IOMMappedReader::new;
            this.writerFactory = IOMMappedWriter::new;
        } else {
            this.readerFactory = IOFileChannelReader::new;
            this.writerFactory = IOFileChannelWriter::new;
        }

        this.clusterSize = dbOptions.getClusterSize();
        this.checksumType = dbOptions.getChecksumType();
        this.compressionType = dbOptions.getCompressionType();
        this.sstFileSize = dbOptions.getSSTSize();
        this.comparator = dbOptions.getComparator();
    }

    public IOReader getIOReader(Path file) throws IOException {
        return readerFactory.create(file);
    }

    public IOWriter getIOWriter(Path filePath, long fileSize) throws IOException {
        return writerFactory.create(filePath, fileSize);
    }

    public byte getClusterSize() {
        return clusterSize;
    }

    public DBConstant.CHECKSUM_TYPE getChecksumType() {
        return this.checksumType;
    }

    public DBConstant.COMPRESSION_TYPE getCompressionType() {
        return this.compressionType;
    }

    public int getSSTSize() {
        return sstFileSize;
    }

    public Comparator<byte[]> getComparator() {
        return comparator;
    }
}
