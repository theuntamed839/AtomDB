package org.example.loom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;

class LoomTest {

    @Test
    void loom() throws Exception {

        var observed = new ConcurrentSkipListSet<String>();

        var threads = IntStream
                .range(0, 100000)
                .mapToObj(index -> Thread.ofVirtual() // ①
                        .unstarted(() -> {
                            var first = index == 0;
                            if (first) {
                                observed.add(Thread.currentThread().toString());
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            if (first) {
                                observed.add(Thread.currentThread().toString());
                            }
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            if (first) {
                                observed.add(Thread.currentThread().toString());
                            }
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            if (first) {
                                observed.add(Thread.currentThread().toString());
                            }
                        }))
                .toList();

        for (var t : threads)
            t.start();

        for (var t : threads)
            t.join();

        System.out.println(observed);

        Assertions.assertTrue(observed.size() > 1);

    }

}
