package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.SSTIO.IOReader;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;

class IndexedClusterIterator implements AutoCloseable {
    private final IOReader reader;
    private final int clusterEndPoint;
    private final SSTInfo sstInfo;
    private final byte singleClusterSize;
    private final ArrayDeque<KVUnit> queue;
    private int retrievedClusterCount = 0;

    public IndexedClusterIterator(SSTInfo sstInfo, DbComponentProvider dbComponentProvider) throws IOException {
        this.sstInfo = Objects.requireNonNull(sstInfo, "SSTInfo cannot be null");
        this.reader = dbComponentProvider.getIOReader(sstInfo.getSstPath());
        this.clusterEndPoint = (int) Math.abs(sstInfo.getPointers().get(sstInfo.getPointers().size() - 1).position());
        this.singleClusterSize = sstInfo.getSingleClusterSize();
        this.queue = new ArrayDeque<>(sstInfo.getSingleClusterSize());
    }

    public boolean hasNext() throws IOException {
        return !queue.isEmpty() || !isFileLimitReached();
    }

    private boolean isFileLimitReached() throws IOException {
        // todo which one comes the first, we can optimize this
        return reader.position() == clusterEndPoint || retrievedClusterCount == sstInfo.getPointers().size();
    }

    public byte[] nextClusterSmallestKey() throws IOException {
        ensureNotAtEnd("No more elements IndexedClusterIterator");
        return sstInfo.getPointers().get(retrievedClusterCount).key();
    }

    public KVUnit getNextKVUnit() throws IOException {
        ensureHasNext("Attempt to fetch KVUnit without available clusters");
        if (queue.isEmpty()) {
            loadNextClusterToQueue();
        }
        return queue.getFirst();
    }

    private void loadNextClusterToQueue() throws IOException {
        ensureNotAtEnd("Cannot load cluster, end of path reached");
        try {
            IndexedCluster.fillQueue(reader, sstInfo.getPointers().get(retrievedClusterCount++), singleClusterSize, sstInfo.getCompressionStrategy(), queue);
        } catch (IOException e) {
            throw new IllegalStateException("Error while reading the next cluster", e);
        }
    }

    public KVUnit pollNextKVUnit() throws IOException {
        if (queue.isEmpty()) {
            loadNextClusterToQueue();
        }
        return queue.pollFirst();
    }

    private void ensureHasNext(String errorMessage) throws IOException {
        if (!hasNext()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private void ensureNotAtEnd(String errorMessage) throws IOException {
        if (isFileLimitReached()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    public SSTInfo getSSTInfo() {
        return sstInfo;
    }

    private void validateStateBeforeClose() throws IOException {
        if (reader.position() != clusterEndPoint) {
            throw new IllegalStateException("File not read completely");
        }
        if (!queue.isEmpty()) {
            throw new IllegalStateException("Queue not empty, current size: " + queue.size());
        }
    }

    @Override
    public void close() throws IOException {
        validateStateBeforeClose();
        try {
            reader.close();
        } catch (IOException e) {
            throw new IllegalStateException("Error closing reader", e);
        }
    }
}