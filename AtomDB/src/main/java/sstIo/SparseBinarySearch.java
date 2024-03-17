package sstIo;

import sst.KeyUnit;
import util.Util;

import java.util.HashMap;
import java.util.Map;

/**
 * Math behind sparse Binary
 *
 *  we do [Math.ceil(Math.log(pointers.size())/Math.log(2)) / 2]
 *  basically half of the number of jumps binary search would require.
 *  these are the most frequentcly access data
 *
 *  we try to store the first half of the jumps
 *  the which turns out 0(2^n) keys, where n is the half of log(number if items) to the base 2
 *
 *  so for 50K keys, to find a key we need approx 16 jumps
 *  16/2 = 8
 *  8 ^ 2 = 256 keys we store in memory
 */
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
