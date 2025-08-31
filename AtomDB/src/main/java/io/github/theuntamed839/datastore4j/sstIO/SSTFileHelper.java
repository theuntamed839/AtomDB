package io.github.theuntamed839.datastore4j.sstIO;

import io.github.theuntamed839.datastore4j.compaction.PointerList;
import io.github.theuntamed839.datastore4j.constants.DBConstant;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.github.theuntamed839.datastore4j.db.DbComponentProvider;
import io.github.theuntamed839.datastore4j.table.SSTFileNameMeta;
import io.github.theuntamed839.datastore4j.table.SSTInfo;

import java.io.IOException;

public class SSTFileHelper {

    public static SSTInfo getSSTInfo(SSTFileNameMeta sstMeta, DbComponentProvider dbComponentProvider) {
        try(var reader = dbComponentProvider.getIOReader(sstMeta.path())) {
            var header = SSTHeader.getHeader(reader);

            reader.position(header.getFilterPosition());
            var bloomFilter = BloomFilter.readFrom(reader, Funnels.byteArrayFunnel());

            reader.position(header.getPointersPosition());
            PointerList pointerList = PointerList.getPointerList(reader, dbComponentProvider.getComparator(), 1 + (int) Math.ceil((header.getNumberOfEntries() * 1.0) / header.getSingleClusterSize()));
            if (reader.getLong() != DBConstant.MARK_FILE_END) {
                throw new IOException("Trouble in reading the file, maybe corrupt." + reader.position());
            }
            return new SSTInfo(header, pointerList, bloomFilter, sstMeta);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
