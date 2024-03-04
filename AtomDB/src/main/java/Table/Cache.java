package Table;

import util.SizeOf;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// todo
// this is very lose cache behaviour

public class Cache {
    private int capacity;
    private Map<String, CacheValue> cache;
    private long currentSize = 0;

    public record CacheValue(byte[] smallKey, byte[] largeKey, List<Long> pointers, long size) {
        public CacheValue(byte[] smallKey, byte[] largeKey, List<Long> pointers) {
            this(smallKey, largeKey, pointers,
                    smallKey.length +
                            largeKey.length +
                            (long) pointers.size() * Long.BYTES
                    );
        }
    };

    public Cache(int capacity) {
        this.capacity = capacity;
        initCache();
    }

    private void initCache() {
        cache = new LinkedHashMap<>(30, 0.75f, true){
            protected boolean removeEldestEntry(Map.Entry eldest) {
                if (currentSize > capacity) {
                    currentSize -= ((CacheValue)eldest.getValue()).size;
                    return true;
                }
                return false;
            }
        };
    }

    public Cache() {
        this.capacity = 100 * SizeOf.MB;
        initCache();
    }

    public int getSize() {
        return capacity;
    }

    public void setSize(int size) {
        this.capacity = size;
    }

    public void put(String filename, byte[] small, byte[] large, List<Long> pointers) {
        CacheValue cacheValue = new CacheValue(small, large, pointers);
        currentSize += cacheValue.size;
        cache.put(filename, cacheValue);
    }

    public CacheValue get(String fileName) {
        return cache.getOrDefault(fileName, null);
    }
}
