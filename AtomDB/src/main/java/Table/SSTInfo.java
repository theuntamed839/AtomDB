package Table;

import com.google.common.hash.BloomFilter;
import sst.Header;
import sstIo.Reader;
import sstIo.SSTReaderWithBuffer;
import sstIo.SparseBinarySearch;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 *  this will contain small and large key, bloom, pointers, and the sparse keys
 */
public class SSTInfo implements AutoCloseable{
    private final BloomFilter<byte[]> bloomFilter;
    private final Header header;
    private final SparseBinarySearch sparseBinarySearch;
    private List<Long> pointers;
    private final String fileName;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private SSTReaderWithBuffer reader;

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

    public SSTReaderWithBuffer getSSTReader() {
        if (reader != null) return reader;
        try{
            randomAccessFile = new RandomAccessFile(getFileName(), "r");
            channel = randomAccessFile.getChannel();
            reader = new SSTReaderWithBuffer(channel);
        } catch (Exception e) {
            System.out.println("Moye moye while searching");
            throw new RuntimeException(e);
        }
        return reader;
    }

    @Override
    public void close() throws Exception {
        reader.close();
        channel.close();
        randomAccessFile.close();
    }
}
