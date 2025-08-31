package io.github.theuntamed839.datastore4j.search;

import org.junit.jupiter.api.*;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class SafeCacheTest {

    static class TestResource implements AutoCloseable {
        private boolean closed = false;
        private final int id;

        TestResource(int id) {
            this.id = id;
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    @Test
    void acquireAndRelease_ShouldReuseSameInstance() throws Exception {
        AtomicInteger loaderCount = new AtomicInteger();
        try (var cache = new SafeCache<>(Runtime.getRuntime().availableProcessors() * 2, _ -> new TestResource(loaderCount.incrementAndGet()))
        ) {
            TestResource r1 = cache.acquire(1);
            cache.release(1);
            TestResource r2 = cache.acquire(1);

            assertSame(r1, r2, "Cache should reuse the same resource for same key");
            assertEquals(1, loaderCount.get(), "Value loader should be called once");
        }
    }

    @Test
    void evict_ShouldCloseResource() throws Exception {
        try (var cache = new SafeCache<>(Runtime.getRuntime().availableProcessors() * 2, TestResource::new)) {
            TestResource r = cache.acquire(1);
            cache.release(1);
            cache.evict(1);

            assertTrue(r.isClosed(), "Resource should be closed after eviction");
        }
    }

    @Test
    void forceEvictAll_ShouldCloseAll() throws Exception {
        SafeCache<Integer, TestResource> cache =
                new SafeCache<>(Runtime.getRuntime().availableProcessors() * 2, TestResource::new);

        TestResource r1 = cache.acquire(1);
        TestResource r2 = cache.acquire(2);

        cache.release(1);
        cache.release(2);
        cache.forceEvictAll();

        assertTrue(r1.isClosed(), "All resources should be closed after forceEvictAll");
        assertTrue(r2.isClosed(), "All resources should be closed after forceEvictAll");
    }

    @Test
    void overLimit_ShouldEvictSomeEntries() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        SafeCache<Integer, TestResource> cache =
                new SafeCache<>(Runtime.getRuntime().availableProcessors() * 2, _ -> new TestResource(counter.incrementAndGet()));

        // Fill cache
        TestResource r1 = cache.acquire(0);
        cache.release(0);
        for (int i = 1; i <= 33; i++) {
            cache.acquire(i);
            cache.release(i);
        }

        assertTrue(r1.isClosed(), "Should be evicted");
    }

    @Test
    void close_ShouldCloseAndClearCache() throws Exception {
        SafeCache<String, TestResource> cache =
                new SafeCache<>(Runtime.getRuntime().availableProcessors() * 2, _ -> new TestResource(1));

        TestResource r = cache.acquire("a");
        cache.release("a");
        cache.close();

        assertTrue(r.isClosed(), "Resource should be closed when cache is closed");
    }

    @Test
    void stressTest() {
        try (var cache =
                     new SafeCache<>(Runtime.getRuntime().availableProcessors() * 2, TestResource::new);
             var executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)) {
            for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 2; i++) {
                executorService.execute(() -> {
                    Random random = new Random();
                    for (int j = 0; j < 1000; j++) {
                        try {
                            TestResource r = cache.acquire(random.nextInt(Runtime.getRuntime().availableProcessors() * 2 + 4));
                            if (r.isClosed()) {
                                Assertions.fail("Resource should not be closed");
                            }
                            cache.release(r.id);
                        } catch (Exception e) {
                            e.printStackTrace();
                            Assertions.fail();
                        }
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

