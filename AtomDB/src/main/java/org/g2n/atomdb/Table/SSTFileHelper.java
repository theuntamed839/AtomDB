package org.g2n.atomdb.Table;

import org.g2n.atomdb.Compaction.PointerList;
import org.g2n.atomdb.Constants.DBConstant;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.g2n.atomdb.sstIo.*;

import java.io.File;

public class SSTFileHelper {

    public static SSTInfo getSSTInfo(File file) {
        try(var reader = new MMappedReader(file)) {
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
}
