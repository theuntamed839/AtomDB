package org.g2n.atomdb.db;

import java.nio.ByteBuffer;

public interface ByteExportable {
    void exportBytes(ExpandingByteBuffer buffer);
}
