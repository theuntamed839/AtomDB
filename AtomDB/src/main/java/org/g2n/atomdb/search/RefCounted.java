package org.g2n.atomdb.search;

import java.util.concurrent.atomic.AtomicInteger;

/*
TODO: If the holding object is heavy it would take time to close and clear its resources.
    We might delegate this closing work to a background thread.
*/

public class RefCounted<V extends AutoCloseable> {
    final V value;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean markedForClose = false;

    RefCounted(V value) {
        this.value = value;
    }

    void retain() {
        refCount.incrementAndGet();
        if (markedForClose) {
            throw new IllegalStateException("Object is being closed");
        }
    }

    void release() {
        refCount.decrementAndGet();
        if (markedForClose) {
            throw new IllegalStateException("Object is being closed");
        }
    }

    void markForClose() throws Exception {
        markedForClose = true;
        value.close();
    }

    public void forceClose() throws Exception {
        markedForClose = true;
        value.close();
    }

    public boolean isClosed() {
        return markedForClose;
    }

    public int getRefCount() {
        return refCount.get();
    }
}