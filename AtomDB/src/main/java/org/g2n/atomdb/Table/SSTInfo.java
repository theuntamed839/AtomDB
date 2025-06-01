package org.g2n.atomdb.Table;

import org.g2n.atomdb.Compaction.PointerList;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import org.g2n.atomdb.SSTIO.SSTHeader;
import org.g2n.atomdb.SSTIO.SSTKeyRange;

import java.nio.file.Files;
import java.nio.file.Path;

public class SSTInfo extends SSTHeader implements Comparable<SSTInfo> {
    private final Path sstPath;
    private final PointerList pointers;
    private final BloomFilter<byte[]> filter;
    private final long number;
    private final SSTKeyRange sstKeyRange;
    private final int sstHashCode;


    public SSTInfo(Path sstPath, SSTHeader header, PointerList pointers, BloomFilter<byte[]> filter, SSTFileNameMeta fileNameMeta) {
        super(header);
        Preconditions.checkArgument(Files.exists(sstPath), "SST file does not exist: " + sstPath);
        this.number = fileNameMeta.seq();
        this.sstPath = sstPath;
        this.sstHashCode = sstPath.toAbsolutePath().hashCode(); // todo do we need to do this ?
        this.pointers = pointers;
        this.filter = filter;
        this.sstKeyRange = new SSTKeyRange(pointers.getFirst().key(), pointers.getLast().key());
    }

    public SSTKeyRange getSstKeyRange() {
        return sstKeyRange;
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

    public int getFileTorsoSize() {
        // todo change this name
        return (int) (getPointersPosition() - pointers.getLast().position());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SSTInfo sstInfo = (SSTInfo) o;
        return sstPath.toAbsolutePath().equals(sstInfo.sstPath.toAbsolutePath());
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
                ", sstKeyRange=" + sstKeyRange.toString() +
                ", sstHashCode=" + sstHashCode +
                ", SSTHeader=" + super.toString() +
                '}';
    }
}
