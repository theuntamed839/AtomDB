package org.g2n.atomdb.Table;

import org.g2n.atomdb.SSTIO.Range;
import org.g2n.atomdb.db.DBComparator;

import java.util.ArrayList;

class ExclusiveSSTs {
    public final ArrayList<SSTInfo> ssts;
    private Range range;

    public ExclusiveSSTs(SSTInfo info) {
        this.ssts = new ArrayList<>();
        this.range = info.getSstKeyRange();
        this.ssts.add(info);
    }

    public boolean insertIfMutuallyExclusive(SSTInfo info) {
        if (range == null) {
            this.range = info.getSstKeyRange();
            this.ssts.add(info);
            return true;
        }

        if (!range.overLapping(info.getSstKeyRange())) {
            insert(info);
            return true;
        }
        return false;
    }

    private void insert(SSTInfo info) {
        ssts.add(info);
        ssts.sort((a1, a2) -> DBComparator.byteArrayComparator.compare(a1.getSstKeyRange().getSmallest(), a2.getSstKeyRange().getSmallest()));
        recalculateRange();
    }

    private void recalculateRange() {
        byte[] greatest = ssts.stream().max((a1, a2) -> DBComparator.byteArrayComparator.compare(a1.getSstKeyRange().getGreatest(), a2.getSstKeyRange().getGreatest())).get().getSstKeyRange().getGreatest();
        this.range = new Range(
                ssts.getFirst().getSstKeyRange().getSmallest(),
                greatest
        );
    }

    public boolean remove(SSTInfo info) {
        if (ssts.remove(info)) {
            if (ssts.isEmpty()) {
                range = null;
            } else {
                recalculateRange();
            }
            return true;
        }
        return false;
    }

    public SSTInfo getSSTContaining(byte[] target) {
        if (range != null && range.inRange(target)) {
            return getSSTContaining_rangeChecked(target);
        }
        return null;
    }

    private SSTInfo getSSTContaining_rangeChecked(byte[] target) {
        int low = 0, high = ssts.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;

            SSTInfo info = ssts.get(mid);
            if (info.getSstKeyRange().inRange(target)) {
                return info;
            } else if (DBComparator.byteArrayComparator.compare(target, info.getSstKeyRange().getSmallest()) < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

}
