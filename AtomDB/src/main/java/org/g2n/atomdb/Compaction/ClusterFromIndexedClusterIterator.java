package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Constants.DBConstant;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.MMappedReader;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

class ClusterFromIndexedClusterIterator implements AutoCloseable {
    private final MMappedReader reader;
    private final int clusterEndPoint;
    private final SSTInfo sstInfo;
    private final byte numberOfKeysInSingleCluster;
    private final ArrayDeque<KVUnit> queue;
    private int numberOfRetrievedClusterCount = 0;

    public ClusterFromIndexedClusterIterator(SSTInfo sstInfo) throws IOException {
        this.reader = new MMappedReader(sstInfo.getSst());
        this.sstInfo = sstInfo;
        this.clusterEndPoint = (int) Math.abs(sstInfo.getPointers().get(sstInfo.getPointers().size() - 1).position());
        this.numberOfKeysInSingleCluster = sstInfo.getNumberOfKeysInSingleCluster();
        this.queue = new ArrayDeque<>(DBConstant.CLUSTER_SIZE);
    }

    public boolean hasNext() {
        return !queue.isEmpty() || !isFileLimitReached();
    }

    public Cluster nextCluster() {
        if (!hasNext()) {
            System.out.println("No more elements ClusterFromIndexedClusterIterator");
            throw new NoSuchElementException();
        }
        try {
            return IndexedCluster.readSimpleCluster(reader, sstInfo.getPointers().get(numberOfRetrievedClusterCount++), numberOfKeysInSingleCluster);
        } catch (IOException e) {
            System.out.println("Error while reading next cluster, are we reached end of file?");
            throw new RuntimeException(e);
        }
    }

    private boolean isFileLimitReached() {
        // todo which one comes the first, we can optimize this
        return reader.position() == clusterEndPoint || numberOfRetrievedClusterCount == sstInfo.getPointers().size();
    }

    @Override
    public void close() {
        // todo remove
        if (clusterEndPoint != reader.position()) {
            throw new RuntimeException("File not read completely");
        }

        if (!queue.isEmpty()) {
            throw new RuntimeException("Queue not empty currentSize=" + queue.size());
        }
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] nextClusterSmallestKey() {
        if (isFileLimitReached()) {
            throw new RuntimeException("No more clusters to read");
        }
        return sstInfo.getPointers().get(numberOfRetrievedClusterCount).key();
    }

    public KVUnit getNextKVUnit() {
        if (!hasNext()) {
            throw new RuntimeException("Why i am called with hasnext");
        }
        if (!queue.isEmpty()) {
            return queue.getFirst();
        }
        try {
            IndexedCluster.fillQueue(reader, sstInfo.getPointers().get(numberOfRetrievedClusterCount++), numberOfKeysInSingleCluster, queue);
        } catch (IOException e) {
            System.out.println("Error while reading next cluster, are we reached end of file?");
            throw new RuntimeException(e);
        }
        return queue.getFirst();
    }

    public SSTInfo getSSTInfo() {
        return sstInfo;
    }

    public KVUnit pollNextKVUnit() {
        if (!queue.isEmpty()) {
            return queue.pollFirst();
        }
        try {
            IndexedCluster.fillQueue(reader, sstInfo.getPointers().get(numberOfRetrievedClusterCount++), numberOfKeysInSingleCluster, queue);
        } catch (IOException e) {
            System.out.println("Error while reading next cluster, are we reached end of file?");
            throw new RuntimeException(e);
        }
        return queue.pollFirst();
    }
}