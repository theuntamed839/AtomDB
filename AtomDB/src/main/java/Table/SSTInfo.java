package Table;

import Compaction.PointerList;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import sstIo.SSTHeader;

import java.io.File;
import java.util.Objects;

public class SSTInfo extends SSTHeader implements AutoCloseable, Comparable<SSTInfo>{

    private final File sst;
    private final PointerList pointers;
    private final BloomFilter<byte[]> filter;

    public SSTInfo(File sst, SSTHeader header, PointerList pointers, BloomFilter<byte[]> filter) {
        super(header);
        Preconditions.checkArgument(sst.exists());
        this.sst = sst;
        this.pointers = pointers;
        this.filter = filter;
    }

    public File getSst() {
        return sst;
    }

    public PointerList getPointers() {
        return pointers;
    }

    public boolean mightContainElement(byte[] key) {
        return filter.mightContain(key);
    }

    @Override
    public int compareTo(SSTInfo sstInfo) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SSTInfo sstInfo = (SSTInfo) o;
        return sst.getAbsolutePath().equals(sstInfo.sst.getAbsolutePath());
    }

    @Override
    public int hashCode() {
        int result = sst != null ? sst.hashCode() : 0;
        result = 31 * result + (pointers != null ? pointers.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }
}
