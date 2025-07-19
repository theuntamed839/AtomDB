package org.g2n.atomdb.search;

public record Checksums(long[] checksums) {
    public int getSize() {
        return checksums.length * Long.BYTES;
    }
}
