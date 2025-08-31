package io.github.theuntamed839.datastore4j.search;

public record Checksums(long[] checksums) {
    public int getSize() {
        return checksums.length * Long.BYTES;
    }
}
