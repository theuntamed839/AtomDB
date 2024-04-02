package Table;

import Compaction.PointerList;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import sstIo.SSTHeader;

import java.io.File;

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

    @Override
    public int compareTo(SSTInfo sstInfo) {

    }
}
