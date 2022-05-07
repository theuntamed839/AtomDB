package Compaction;

import java.util.*;

public class Test {

    public Map<Integer, Map<String, String>> levels;

    private Comparator<String> treeComparator = (s1, s2) -> {
        long s1L = Long.parseLong(s1.substring(0, s1.length() - 26));
        long s2L = Long.parseLong(s2.substring(0, s1.length() - 26));
        return Long.compare(s1L, s2L);
    };

    public Test() {
        this.levels = new HashMap<>();
        levels.put(0, new TreeMap<>(treeComparator));
        levels.put(1, new TreeMap<>(treeComparator));
        levels.put(2, new TreeMap<>(treeComparator));
        levels.put(3, new TreeMap<>(treeComparator));
        levels.put(4, new TreeMap<>(treeComparator));
        levels.put(5, new TreeMap<>(treeComparator));
        levels.put(6, new TreeMap<>(treeComparator));
        levels.put(7, new TreeMap<>(treeComparator));
    }

    public static void main(String[] args) {
        var test = new Test();
        test.levels.get(0).put("40582022-04-13T21:06:50.141167287","2022-04-13T21:07:03.659833229");
        test.levels.get(0).put("81342022-04-13T21:07:03.654660161","Level-0-4.sst");
        test.levels.get(0).put("40582022-04-13T21:07:49.010241294", "2022-04-13T21:07:03.659833229");
        System.out.println(test.levels.get(0));
    }
}
