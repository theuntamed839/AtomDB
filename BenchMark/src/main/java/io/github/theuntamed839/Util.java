package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Util {
    public static List<byte[]> fillDB(BenchmarkDBAdapter db, int entryCount, int keySize, int valueSize, long seed) throws Exception {
        var keys = new ArrayList<byte[]>(entryCount);
        var random = new Random(seed);
        for (int i = 0; i < entryCount; i++) {
            var key = new byte[keySize];
            var value = new byte[valueSize];
            random.nextBytes(key);
            random.nextBytes(value);

            keys.add(key);
            db.put(key, value);
        }

        return keys;
    }

    public static List<byte[]> fillDB(BenchmarkDBAdapter db, int entryCount, int maxKeySize, int minKeySize, int maxValueSize, int minValueSize, long seed) throws Exception {
        var keys = new ArrayList<byte[]>(entryCount);
        var random = new Random(seed);
        for (int i = 0; i < entryCount; i++) {
            var key = new byte[random.nextInt(minKeySize, maxKeySize)];
            var value = new byte[random.nextInt(minValueSize, maxValueSize)];
            random.nextBytes(key);
            random.nextBytes(value);

            keys.add(key);
            db.put(key, value);
        }

        return keys;
    }
}
