package org.g2n.atomdb.table;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class BinarySearchHolderImpl implements SSTHolder {
    private final Deque<ExclusiveSSTs> list = new ConcurrentLinkedDeque<>();

    @Override
    public Collection<SSTInfo> getSSTsContaining(byte[] key) {
        var result = new ArrayList<SSTInfo>();
        for (ExclusiveSSTs ssts : list) {
            var overlapping = ssts.getSSTContaining(key);
            if (overlapping != null) {
                result.add(overlapping);
            }
        }
        result.sort(Comparator.naturalOrder());
        return result;
    }

    @Override
    public void add(SSTInfo info) {
        for (ExclusiveSSTs ssts : list) {
            if (ssts.insertIfMutuallyExclusive(info)) {
                return;
            }
        }
        list.add(new ExclusiveSSTs(info));
    }

    @Override
    public void remove(SSTInfo info) {
        for (ExclusiveSSTs ssts : list) {
            ssts.remove(info);
        }
    }
}
