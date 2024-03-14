package sstIo;

import sst.KeyUnit;
import util.Util;

import java.util.HashMap;
import java.util.Map;

public class SparseBinarySearch {
    private Map<Long, KeyUnit> map;

    public SparseBinarySearch(int entries) {
        map = new HashMap<>(entries);
    }

    public void put(long pointer, KeyUnit keyUnit) {
        Util.requiresNull(map.put(pointer, keyUnit), "Moye Moye ho gaya");
    }

    public KeyUnit get(long pointer) {
        return map.get(pointer);
    }
}
