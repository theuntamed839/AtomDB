package io.github.theuntamed839.datastore4j.db;

import io.github.theuntamed839.datastore4j.constants.DBConstant;
import io.github.theuntamed839.datastore4j.sstIO.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

public final class DbComponentProvider {
    private final byte clusterSize;
    private final IOReaderFactory readerFactory;
    private final IOWriterFactory writerFactory;
    private final DBConstant.CHECKSUM_TYPE checksumType;
    private final DBConstant.COMPRESSION_TYPE compressionType;
    private final long SSTFileSize;
    private final Comparator<byte[]> comparator;
    private final long keyValueCacheSize;
    private final long memtableSize;

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
        this.SSTFileSize = dbOptions.getSSTFileSize();
        this.comparator = dbOptions.getComparator();
        this.keyValueCacheSize = dbOptions.getKeyValueCacheSize();
        this.memtableSize = dbOptions.getMemtableSize();
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

    public long getSSTFileSize() {
        return SSTFileSize;
    }

    public Comparator<byte[]> getComparator() {
        return comparator;
    }

    public long getKeyValueCacheSize() {
        return keyValueCacheSize;
    }

    public long getMemtableSize() {
        return memtableSize;
    }
}
