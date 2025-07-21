package org.g2n.atomdb.table;

import org.g2n.atomdb.level.Level;
import org.g2n.atomdb.sstIO.Range;
import org.g2n.atomdb.db.DBComparator;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class BasicSSTHolderImpl implements SSTHolder{

    private final Map<Level, List<SSTInfo>> fileList = new ConcurrentSkipListMap<>();
    private final Map<Level, Range> rangeMap = new HashMap<>();

    @Override
    public Collection<SSTInfo> getSSTsContaining(byte[] key) {
        var result = new ArrayList<SSTInfo>();
        for (Level level : fileList.keySet()) {
            Range range = rangeMap.get(level);
            if (range != null && range.inRange(key)) {
                for (SSTInfo sstInfo : fileList.get(level)) {
                    if (sstInfo.getSstKeyRange().inRange(key)) {
                        result.add(sstInfo);
                    }
                }
            }
        }
        result.sort(Comparator.naturalOrder());
        return result;
    }

    @Override
    public void add(SSTInfo info) {
        fileList.computeIfAbsent(info.getLevel(), _ -> new ArrayList<>())
                .add(info);
        Range range = rangeMap.getOrDefault(info.getLevel(), null);
        if (range == null) {
            rangeMap.put(info.getLevel(), info.getSstKeyRange());
        } else {
            byte[] low = DBComparator.byteArrayComparator.compare(range.getSmallest(), info.getSstKeyRange().getSmallest()) < 0
                    ? range.getSmallest() : info.getSstKeyRange().getSmallest();
            byte[] high = DBComparator.byteArrayComparator.compare(range.getGreatest(), info.getSstKeyRange().getGreatest()) > 0
                    ? range.getGreatest() : info.getSstKeyRange().getGreatest();
//            byte[] low = range.inRange(info.getSstKeyRange().getSmallest()) ? range.getSmallest() : info.getSstKeyRange().getSmallest();
//            byte[] high = range.inRange(info.getSstKeyRange().getGreatest()) ? range.getGreatest() : info.getSstKeyRange().getGreatest();
            rangeMap.put(info.getLevel(), new Range(low, high));
        }
    }

    @Override
    public void remove(SSTInfo info) {
        if (fileList.getOrDefault(info.getLevel(), new ArrayList<>()).remove(info)) {
            if (fileList.get(info.getLevel()).isEmpty()) {
                rangeMap.put(info.getLevel(), null);
            } else {
                List<SSTInfo> sstInfos = fileList.get(info.getLevel());
                sstInfos.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getSstKeyRange().getSmallest(), b.getSstKeyRange().getSmallest()));
                byte[] smallest = sstInfos.getFirst().getSstKeyRange().getSmallest();
                sstInfos.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getSstKeyRange().getGreatest(), b.getSstKeyRange().getGreatest()));
                byte[] greatest = sstInfos.getLast().getSstKeyRange().getGreatest();

                rangeMap.put(info.getLevel(), new Range(smallest, greatest));
            }
        }
    }
}
