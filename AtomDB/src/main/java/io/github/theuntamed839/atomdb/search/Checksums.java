package io.github.theuntamed839.atomdb.search;

public record Checksums(long[] checksums) {
    public int getSize() {
        return checksums.length * Long.BYTES;
    }
}
