package io.github.theuntamed839.datastore4j.search;

import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

/*
Note:
I wanted the cache to be aware of the reference to the value which was acquired from the cache. So that it can avoid closing this object and move to next, when auto evicting.
Otherwise in a multithreaded environment, it could happen that one thread acquires a value, and cache maybe try to evict the same value,
 which would lead to closing the value while it is still in use.

https://stackoverflow.com/questions/79733518/how-do-i-properly-invalidate-and-clear-a-caffeine-cache
 */

public class SafeCache<K, V extends AutoCloseable> implements AutoCloseable{
    private final ConcurrentHashMap<K, RefCounted<V>> cache = new ConcurrentHashMap<>();
    private final int maxSize;
    private final Function<K, V> valueLoader;
    private final StampedLock lock = new StampedLock();

    public SafeCache(int maxSize, Function<K, V> valueLoader) {
        if (maxSize < Runtime.getRuntime().availableProcessors() * 2) {
            throw new IllegalArgumentException("maxSize must be greater than available thread count");
        }
        this.maxSize = maxSize;
        this.valueLoader = valueLoader;
    }

    public V acquire(K key) throws Exception {
        evictIfOverLimit();
        long readStamp = lock.readLock();
        try {
            RefCounted<V> existing = cache.compute(key, (_, value) -> { // returns old value.
                if (value == null || value.isClosed()) {
                    try {
                        value = new RefCounted<>(valueLoader.apply(key));
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }
                value.retain();
                return value;
            });
            return existing.value;
        }finally {
            lock.unlockRead(readStamp);
        }
    }

    public V get(K key) throws Exception {
        return acquire(key);
    }

    public void release(K key) {
        long stamp = lock.readLock();
        try {
            RefCounted<V> ref = cache.get(key);
            if (ref != null) {
                ref.release();
            }
        }finally {
            lock.unlockRead(stamp);
        }
    }

    private void evictIfOverLimit() throws Exception {
        long l = lock.tryOptimisticRead();
        if (cache.size() < maxSize && lock.validate(l)) {
            return; // no need to evict
        }
        long stamp = lock.writeLock();
        try {
            if (cache.size() > maxSize) {
                int deletedCount = 0;
                Enumeration<K> keys = cache.keys();
                while(cache.size() > Math.floor(maxSize * 0.9) && keys.hasMoreElements()) {
                    K keyToEvict = keys.nextElement();
                    if (keyToEvict == null) {
                        continue;
                    }
                    RefCounted<V> vRefCounted = cache.get(keyToEvict);
                    if (vRefCounted == null) {
                        continue;
                    }
                    if (vRefCounted.isClosed()) {
                        cache.remove(keyToEvict);
                        deletedCount++;
                        continue;
                    }
                    if (vRefCounted.getRefCount() == 0) {
                        deletedCount++;
                        cache.remove(keyToEvict);
                        vRefCounted.markForClose();
                    }
                }
                if (deletedCount == 0) {
                    throw new RuntimeException("Reference to values are still held, unable to evict");
                }
            }
        }finally {
            lock.unlockWrite(stamp);
        }
    }

    public void forceEvictAll() {
        long stamp = lock.writeLock();
        try {
            cache.values().forEach(ref -> {
                try {
                    ref.forceClose();
                } catch (Exception e) {
                    throw new RuntimeException("Error while closing resource", e);
                }
            });
            cache.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void evict(K key) throws Exception {
        long stamp = lock.writeLock();
        try {
            RefCounted<V> ref = cache.remove(key);
            if (ref != null) {
                ref.markForClose();
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void evictAll(Collection<K> toRemove) throws Exception {
        long stamp = lock.writeLock();
        try {
            for (K key : toRemove) {
                if (key == null) {
                    continue; // skip null keys
                }

                RefCounted<V> ref = cache.remove(key);
                if (ref != null) {
                    ref.markForClose();
                }
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void close() throws Exception {
        long stamp = lock.writeLock();
        try {
            for (RefCounted<V> ref : cache.values()) {
                ref.markForClose();
            }
            cache.clear();
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
