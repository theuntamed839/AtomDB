package io.github.theuntamed839.datastore4j.table;

import io.github.theuntamed839.datastore4j.compaction.PointerList;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import io.github.theuntamed839.datastore4j.sstIO.Range;
import io.github.theuntamed839.datastore4j.sstIO.SSTHeader;

import java.nio.file.Files;
import java.nio.file.Path;

public class SSTInfo extends SSTHeader implements Comparable<SSTInfo> {
    private final Path sstPath;
    private final SSTFileNameMeta fileNameMeta;
    private final PointerList pointers;
    private final BloomFilter<byte[]> filter;
    private final long number;
    private final Range range;
    private final int sstHashCode;
    private final Path sstNormalizedPath;

    public SSTInfo(SSTHeader header, PointerList pointers, BloomFilter<byte[]> filter, SSTFileNameMeta fileNameMeta) {
        super(header);
        this.sstPath = fileNameMeta.path();
        this.fileNameMeta = fileNameMeta;
        Preconditions.checkArgument(Files.exists(sstPath), "SST file does not exist: " + sstPath);
        Preconditions.checkNotNull(pointers, "Pointers cannot be null");
        Preconditions.checkNotNull(filter, "Filter cannot be null");
        Preconditions.checkNotNull(fileNameMeta, "File name meta cannot be null");

        this.number = fileNameMeta.seq();
        this.sstHashCode = sstPath.toAbsolutePath().normalize().hashCode();
        this.sstNormalizedPath = sstPath.toAbsolutePath().normalize();
        this.pointers = pointers;
        this.filter = filter;
        this.range = new Range(pointers.getFirst().key(), pointers.getLast().key());
    }

    public Range getSstKeyRange() {
        return range;
    }

    public Path getSstPath() {
        return sstPath;
    }

    public PointerList getPointers() {
        return pointers;
    }

    public boolean mightContainElement(byte[] key) {
        return filter.mightContain(key);
    }

    @Override
    public int compareTo(SSTInfo sstInfo) {
        if (this.getLevel().equals(sstInfo.getLevel())) {
            return Long.compare(sstInfo.number, this.number); // newer fileToWrite will have greater number, and they should come first.
        }
        return  Byte.compare(this.getLevel().value(), sstInfo.getLevel().value());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SSTInfo sstInfo)){
            return false;
        }
        return sstNormalizedPath.equals(sstInfo.sstNormalizedPath);
    }

    @Override
    public int hashCode() {
        return sstHashCode;
    }

    @Override
    public String toString() {
        return "SSTInfo{" +
                "sstPath=" + sstPath +
                ", pointers=" + pointers.toString() +
                ", filter=" + filter +
                ", number=" + number +
                ", range=" + range.toString() +
                ", sstHashCode=" + sstHashCode +
                ", SSTHeader=" + super.toString() +
                '}';
    }

    public boolean isNewerThan(SSTInfo sst) {
        return compareTo(sst) < 0;
    }

    public boolean isOlderThan(SSTInfo sst) {
        return compareTo(sst) > 0;
    }

    public SSTFileNameMeta getFileNameMeta() {
        return fileNameMeta;
    }
}
