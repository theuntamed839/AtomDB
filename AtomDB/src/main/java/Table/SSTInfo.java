package Table;

import com.google.common.hash.BloomFilter;
import sst.Header;
import sstIo.SparseBinarySearch;

import java.util.List;

/**
 *  this will contain small and large key, bloom, pointers, and the sparse keys
 */
public class SSTInfo {
    private final BloomFilter<byte[]> bloomFilter;
    private final Header header;
    private final SparseBinarySearch sparseBinarySearch;
    private List<Long> pointers;
    private final String fileName;

    public SSTInfo(String file, Header header, List<Long> pointers, BloomFilter<byte[]> bloomFilter, SparseBinarySearch sparseBinarySearch) {
        this.fileName = file;
        this.header = header;
        this.pointers = pointers;
        this.bloomFilter = bloomFilter;
        this.sparseBinarySearch = sparseBinarySearch;
    }

    public BloomFilter<byte[]> getBloomFilter() {
        return bloomFilter;
    }

    public Header getHeader() {
        return header;
    }

    public SparseBinarySearch getSparseBinarySearch() {
        return sparseBinarySearch;
    }

    public List<Long> getPointers() {
        return pointers;
    }

    public String getFileName() {
        return fileName;
    }
}
