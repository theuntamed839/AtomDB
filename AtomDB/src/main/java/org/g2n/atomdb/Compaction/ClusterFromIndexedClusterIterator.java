package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.sstIo.MMappedReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

class ClusterFromIndexedClusterIterator implements Iterator<Cluster>, AutoCloseable {
    private final MMappedReader reader;
    private final int clusterEndPoint;
    private final SSTInfo sstInfo;
    private final byte numberOfKeysInSingleCluster;
    private int numberOfRetrievedClusterCount = 0;

    public ClusterFromIndexedClusterIterator(SSTInfo sstInfo) throws IOException {
        this.reader = new MMappedReader(sstInfo.getSst());
        this.sstInfo = sstInfo;
        this.clusterEndPoint = (int) Math.abs(sstInfo.getPointers().get(sstInfo.getPointers().size() - 1).position());
        this.numberOfKeysInSingleCluster = sstInfo.getNumberOfKeysInSingleCluster();
    }

    @Override
    public boolean hasNext() {
        return !isFileLimitReached();
    }

    @Override
    public Cluster next() {
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
        return reader.position() == clusterEndPoint || numberOfRetrievedClusterCount == sstInfo.getPointers().size();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public byte[] nextClusterSmallestKey() {
        if (isFileLimitReached()) {
            throw new RuntimeException("No more clusters to read");
        }
        return sstInfo.getPointers().get(numberOfRetrievedClusterCount).key();
    }
}