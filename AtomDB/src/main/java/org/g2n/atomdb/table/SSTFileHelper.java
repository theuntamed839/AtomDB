package org.g2n.atomdb.table;

import org.g2n.atomdb.compaction.PointerList;
import org.g2n.atomdb.constants.DBConstant;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.g2n.atomdb.db.DbComponentProvider;
import org.g2n.atomdb.sstIO.*;

public class SSTFileHelper {

    public static SSTInfo getSSTInfo(SSTFileNameMeta sstMeta, DbComponentProvider dbComponentProvider) {
        // todo need to fix this code
        try(var reader = dbComponentProvider.getIOReader(sstMeta.path())) {
            var header = SSTHeader.getHeader(reader);
            System.out.println(header);
            reader.position(header.getFilterPosition());
            BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(reader, Funnels.byteArrayFunnel());
            reader.position(header.getPointersPosition());
            PointerList list = PointerList.getPointerList(reader, 1 + (int) (Math.ceil((header.getNumberOfEntries() * 1.0) / header.getSingleClusterSize())));
            if (reader.getLong() != DBConstant.MARK_FILE_END) {
                System.out.println(list.size());
                throw new Exception("File read wrong "+ reader.position());
            }
            return new SSTInfo(sstMeta.path(), header, list, bloomFilter, sstMeta); // todo why provide the same sstMeta.Path to SSTInfo where we already passing sstmeta
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
