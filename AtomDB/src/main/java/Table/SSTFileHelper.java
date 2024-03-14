package Table;

import Checksum.CheckSum;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import sst.Header;
import sst.KeyUnit;
import sst.MiddleBlock;
import sst.ValueUnit;
import sstIo.*;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.function.Function;

public class SSTFileHelper {

    public static SSTInfo getSSTInfo(String file) {
        try(
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                FileChannel channel = randomAccessFile.getChannel();
                FileLock lock = channel.lock();
                Reader reader = new SSTReaderWithBuffer(channel);
        ) {
            Header header = Header.getHeader(reader);

            List<Long> pointers = new ArrayList<>(header.getEntries());
            ByteBuffer byteBuffer = reader.readSize(new byte[header.getEntries()], header.getBinarySearchLocation(), header.getEntries());
            for (int i = 0; i < header.getEntries(); i++) {
                pointers.add(byteBuffer.getLong());
            }

            BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(reader, Funnels.byteArrayFunnel());

            SparseBinarySearch sparseBinarySearch = getSparseBinarySearch(reader, pointers);

            return new SSTInfo(file, header, pointers, bloomFilter, sparseBinarySearch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SparseBinarySearch getSparseBinarySearch(Reader reader, List<Long> pointers) {
        Function<Integer, KeyUnit> keyRetriever = idk -> MiddleBlock.getKeyUnit(reader, pointers.get(idk));
        return sparseBinarySearch(pointers, keyRetriever);
    }

    public static SparseBinarySearch getSparseBinarySearch(List<Long> pointers, SortedMap<byte[], ValueUnit> map) {
        // todo improvement needed here
        ArrayList<byte[]> keyList = new ArrayList<>(map.keySet());
        Function<Integer, KeyUnit> keyRetriever = idk -> {
            byte[] bytes = keyList.get(idk);
            return MiddleBlock.getKeyUnit(bytes, map.get(bytes));
        };
        return sparseBinarySearch(pointers, keyRetriever);
    }

    private static SparseBinarySearch sparseBinarySearch(List<Long> pointers, Function<Integer, KeyUnit> keyRetriever) {
        int iterations = (int) Math.ceil(Math.log(pointers.size())/Math.log(2));
        var sparse = new SparseBinarySearch(pointers.size());
        getFrequentlyUsedValues(keyRetriever, pointers, 0, pointers.size() - 1, sparse, (int) Math.ceil(iterations * 0.5));
        return sparse;
    }

    private static void getFrequentlyUsedValues (Function<Integer, KeyUnit> keyRetriever, List<Long> pointers,
                                                 int low, int high, SparseBinarySearch sparse, int iterations) {
        if (iterations <= 0) return;

        int mid = (low + high) >>> 1;
        if (mid < 0 || pointers.size() <= mid) return;
        sparse.put(pointers.get(mid), keyRetriever.apply(mid));

        getFrequentlyUsedValues(keyRetriever, pointers, low, mid - 1, sparse, iterations - 1);
        getFrequentlyUsedValues(keyRetriever, pointers, mid + 1, high, sparse, iterations - 1);
    }
}