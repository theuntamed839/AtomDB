package org.example.guava;

import com.google.common.collect.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RangedMapMain {
    public static void main(String[] args) {
        RangeMap<Integer, String> rangeMap = TreeRangeMap
                .create();
        rangeMap.put(Range.closedOpen(0, 4), "0-4");
        rangeMap.put(Range.closedOpen(4, 8), "4-8");
        rangeMap.put(Range.closedOpen(8, 12), "8-12");
        System.out.println(rangeMap.get(4));
//        rangeMap.get
        RangeSet<Integer> a = TreeRangeSet.create();
        a.add(Range.closed(0, 4));
//        a.add(Range.closed(2, 4));
//        a.add(Range.closed(3, 4));
        System.out.println(a.rangeContaining(4));

        List<byte[]> holder = new ArrayList<>();
        for (int i = 0; i < 1000_000; i++) {
            holder.add((i + "").getBytes());
        }
        long start, end;
        start = System.nanoTime();
        List<byte[]> hardList = new ArrayList<>();
        for (int i = 0; i < 1000_000; i++) {
            hardList.add((i + "").getBytes());
        }
        hardList.sort(Arrays::compare);
        end = System.nanoTime();
        System.out.println("took="+(end-start));
        start = System.nanoTime();
        RangeMap<MyByte, String> map = TreeRangeMap.create();
        for (int i = 0; i < 1000_000 - 1; i++) {
            map.put(Range.closedOpen(new MyByte(holder.get(i)), new MyByte(holder.get(i + 1))), "");
        }
        end = System.nanoTime();
        System.out.println("took="+(end-start));


        start = System.nanoTime();
        for (int i = 0; i < 1000_000; i++) {
            String s = map.get(new MyByte(holder.get(i)));
            assert Objects.requireNonNull(s).equalsIgnoreCase("");
        }
        end = System.nanoTime();
        System.out.println("took="+(end-start));

        start = System.nanoTime();
        for (int i = 0; i < 1000_000; i++) {
            int l = 0, h = hardList.size() - 1;
            while(l <= h) {
                int mid = (l + h) >>> 1;
                int compare = Arrays.compare(hardList.get(mid), holder.get(i));
                if (compare < 0){
                    l = mid + 1;
                }
                else if (compare > 0) {
                    h = mid - 1;
                }
                else {
                    assert Arrays.compare(holder.get(i), hardList.get(mid)) ==0;
                    break;
                }
            }
        }
        end = System.nanoTime();
        System.out.println("took="+(end-start));

//        start = System.nanoTime();
//        end = System.nanoTime();
//        System.out.println("took="+(end-start));
    }
}

