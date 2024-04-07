package Compaction;

import Constants.DBConstant;
import Table.SSTInfo;
import com.google.common.collect.PeekingIterator;
import db.DBComparator;
import db.KVUnit;
import sstIo.BufferedMMappedReader;

import javax.naming.OperationNotSupportedException;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class CollectiveSStIterator implements Iterator<KVUnit>{

    private final Collection<SSTInfo> files;
    private final List<PeekingIterator<KVUnit>> iterators;
    private PeekingIterator<KVUnit> currentUnitIterator;

    public CollectiveSStIterator(Collection<SSTInfo> collection) throws IOException {
        this.files = collection;
        this.iterators = new ArrayList<>(collection.size());
        for (SSTInfo sstInfo : collection) {
            iterators.add(new ClusterIterator(sstInfo.getSst(), sstInfo.getPointers(), sstInfo.getClusterKeyCount()));
        }

    }

    @Override
    public boolean hasNext() {
        load();
        return currentUnitIterator != null;
    }

    @Override
    public KVUnit next() {
        load();
        KVUnit next = currentUnitIterator.next();
        currentUnitIterator = null;
        return next;
    }

    private void load() {
        if (currentUnitIterator != null) {
            return;
        }
        List<PeekingIterator<KVUnit>> toRemove = new ArrayList<>();

        currentUnitIterator = null;
        for (PeekingIterator<KVUnit> iterator : iterators) {
            if (iterator.hasNext()) {
                if (currentUnitIterator == null || DBComparator.byteArrayComparator.compare(
                        currentUnitIterator.peek().getKey(), iterator.peek().getKey()) > 0) {
                    currentUnitIterator = iterator;
                }
            } else  {
                toRemove.add(iterator);
            }
        }
        for (PeekingIterator<KVUnit> iterator : iterators) {
            System.out.println("i am here");
            try {
                ((ClusterIterator) iterator).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        iterators.removeAll(toRemove);
    }
}


class ClusterIterator implements PeekingIterator<KVUnit>, AutoCloseable {
    private final File file;
    private final BufferedMMappedReader reader;
    private final PointerList pointerList;
    private final int clusterEndPoint;
    private final IndexedCluster indexedCluster;
    private Iterator<KVUnit> currentIterator;
    private KVUnit peek;
    private int retrievedCount = 0;

    public ClusterIterator(File file, PointerList pointerList, int oneClusterSize) throws IOException {
        this.file = file;
        this.reader = new BufferedMMappedReader(file);
        reader.position((int) pointerList.get(0).position());
        this.pointerList = pointerList;
        this.clusterEndPoint = (int) Math.abs(pointerList.get(pointerList.size() - 1).position());
        this.indexedCluster = new IndexedCluster(oneClusterSize);
        this.peek = null;
        this.currentIterator = null;
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

    private boolean fileLimitReached() throws IOException {
       return reader.position() == clusterEndPoint || retrievedCount == pointerList.size();
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

    private void load() throws IOException {
        if (currentIterator != null && currentIterator.hasNext()) {
            return;
        }
        if (fileLimitReached()) {
            currentIterator = null;
            return;
        }
        currentIterator = indexedCluster.read(reader, pointerList.get(retrievedCount++).key()).getUnits().iterator();
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