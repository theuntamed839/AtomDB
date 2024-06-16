package Compaction;

import Table.SSTInfo;
import com.google.common.collect.PeekingIterator;
import db.KVUnit;
import sstIo.BufferedMMappedReader;
import java.io.IOException;
import java.util.Iterator;

class SSTIterator implements PeekingIterator<KVUnit>, AutoCloseable {
    private final BufferedMMappedReader reader;
    private final int clusterEndPoint;
    private final IndexedCluster indexedCluster;
    private final SSTInfo sstInfo;
    private Iterator<KVUnit> currentIterator;
    private KVUnit peek;
    private int numberOfRetrievedClusterCount = 0;

    public SSTIterator(SSTInfo sstInfo) throws IOException {
        this.reader = new BufferedMMappedReader(sstInfo.getSst());
        this.sstInfo = sstInfo;
        this.clusterEndPoint = (int) Math.abs(sstInfo.getPointers().get(sstInfo.getPointers().size() - 1).position());
        this.indexedCluster = new IndexedCluster(sstInfo.getClusterKeyCount());
    }

    @Override
    public boolean hasNext() {
        try {
            if (peek != null) {
                return true;
            }
            load();
            return currentIterator != null && currentIterator.hasNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KVUnit peek() {
        if (!hasNext()) {
            throw new RuntimeException("Why i was called before hasNext");
        }
        try {
            if (peek != null) {
                return peek;
            }
            load();
            peek = currentIterator.next();
            return peek;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KVUnit next() {
        try {
            if (peek != null){
                var temp = peek;
                peek = null;
                return temp;
            }
            load();
            return currentIterator.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean fileLimitReached() throws IOException {
        return reader.position() == clusterEndPoint || numberOfRetrievedClusterCount == sstInfo.getPointers().size();
    }

    private void load() throws IOException {
        if (currentIterator != null && currentIterator.hasNext()) {
            return;
        }
        if (fileLimitReached()) {
            currentIterator = null;
            reader.close();
            return;
        }
        Cluster cluster = indexedCluster.read(reader, sstInfo.getPointers().get(numberOfRetrievedClusterCount++));
        currentIterator = cluster.getUnits().iterator();
    }

    @Override
    public void remove() {
        throw new RuntimeException("Wait we don't need this");
    }

    @Override
    public void close() throws Exception {
       reader.close();
    }
}