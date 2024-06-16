package Table;

import Compaction.PointerList;
import Constants.DBConstant;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import sst.Header;
import sst.KeyUnit;
import sst.MiddleBlock;
import sst.ValueUnit;
import sstIo.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.Function;

public class SSTFileHelper {

    public static SSTInfo getSSTInfo(File file) {
        try(var reader = new BufferedMMappedReader(file)) {
            System.out.println("File="+file.getName());
            var header = SSTHeader.getHeader(reader);
            System.out.println(header);
            reader.position(header.getFilterPosition());
            BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(reader, Funnels.byteArrayFunnel());
            reader.position(header.getPointersPosition());
            PointerList list = PointerList.getPointerList(reader, 1 + (int) (Math.ceil((header.getNumberOfEntries() * 1.0) / DBConstant.CLUSTER_SIZE)));
            if (reader.getLong() != DBConstant.MARK_FILE_END) {
                System.out.println(list.size());
                throw new Exception("File read wrong "+ reader.position());
            }
            return new SSTInfo(file, header, list, bloomFilter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SparseBinarySearch getSparseBinarySearch(ReaderInterface reader, List<Long> pointers) {
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
        getFrequentlyUsedValues(keyRetriever, pointers, 0, pointers.size() - 1, sparse, (int) Math.ceil(iterations * DBConstant.SPARSE_BINARY_KEY_PERCENTAGE));
        return sparse;
//
//        // whole pointer set in memory
//        var sparse = new SparseBinarySearch(pointers.size());
//        for (int i = 0; i < pointers.size(); i++) {
//            sparse.put(pointers.get(i), keyRetriever.apply(i));
//        }
//        return sparse;

//        int iterations = (int) Math.floor(Math.log(pointers.size())/Math.log(2));
//        var sparse = new SparseBinarySearch(pointers.size());
//        getFrequentlyUsedValues(keyRetriever, pointers, 0, pointers.size() - 1, sparse, iterations);
//        return sparse;
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
