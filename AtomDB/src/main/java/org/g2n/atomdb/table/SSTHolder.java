package org.g2n.atomdb.table;

import java.util.Collection;

public interface SSTHolder {
    Collection<SSTInfo> getSSTsContaining(byte[] key);

    void add(SSTInfo info);

    void remove(SSTInfo info);
}
