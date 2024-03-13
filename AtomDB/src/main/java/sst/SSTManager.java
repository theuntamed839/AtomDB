package sst;

import Compaction.Compaction;
import Constants.DBConstant;
import Level.Level;
import Table.Cache;
import Table.SSTFileHelper;
import Table.SSTInfo;
import Table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.DBComparator;
import db.DBOptions;
import org.xerial.snappy.Snappy;
import sstIo.Reader;
import sstIo.SSTReaderWithBuffer;
import sstIo.SSTWriteWithBuffer;
import sstIo.Writer;
import util.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * current SST Structure
 * BS-> Binary Search position pointer
 * CH-> checksum
 * EN-> number of entries in the sst
 * VID-> version id of that sst
 * LEN-> length
 * SK-> smallest Key
 * LK-> largest Key
 * LEV-> level of that sst
 * K-> key
 * V-> value
 * MAR-> MARKER
 * P->pointer to key
 * Block_LEN -> we going to write a big block so its size
 * VB_LEN -> value block length
 * KB_LEN -> Key block length
 *
 * blocks should be compressed.
 * [ {VID | LEV | BS | EN | SK_LEN | SK | SK_CH | LK_LEN | LK | LK_CH}=header
 *   {K_LEN | K | MAR | V_LEN | V | CH}=middleBlock ideally, btw this will be multiple
 *   {K_LEN | K | MAR | CH}=middleBlock when key is deleted
 *   {P1,P2,P3.....}
 *   {bloom} ]=End
 *
 *
 * NEW
 * [
 *  Header [
 *              VID | LEV | BS | EN | Block_LEN | [ SK_LEN | SK | LK_LEN | LK | CH ]
 *         ]
 *  Body  [
 *              { VB_LEN | V | V_CH | K_Checksum } * N times
 *        ]
 *        [
 *              { KB_LEN | K | K_Checksum | MAR | Value_Pointer} * N times
 *        ]
 *        [
 *              { P1, P2, P3 ....} * N times
 *        ]
 *  Bloom [
 *              Bloom
 *        ]
 * ]
 *
 */

public class SSTManager {
    private final File dbFolder;
    //    private static final byte[] VersionIDBytes = bytes(VersionID);
    private DBOptions dbOptions;
    private Table table;
    // todo need to move cache to somewhere, maybe in table
    private Cache cache;

    //todo need a better place for Compaction
    private Compaction compaction;
    public SSTManager(File dbFolder) {
        this.dbFolder = dbFolder;
        this.table = new Table(dbFolder);
        this.cache = new Cache();
        this.compaction = new Compaction(dbFolder, table);
    }

    public void createSST(SortedMap<byte[], ValueUnit> map) throws Exception {
        // debug
        long a, b;
        a = System.nanoTime();

        String tempFileName = dbFolder.getAbsolutePath() + File.separator +
                Instant.now().toString().replace(':', '_') + Level.LEVEL_ZERO;

        var header = new Header(map.firstKey(), map.lastKey(), map.size(),
                DBConstant.SST_VERSION, Level.LEVEL_ZERO, tempFileName);

        BloomFilter<byte[]> filter = BloomFilter.create(
                Funnels.byteArrayFunnel(),
                map.size(),
                0.01);
        List<Long> pointers = new ArrayList<>(map.size());
        try(
                RandomAccessFile file = new RandomAccessFile(tempFileName, "rw");
                FileChannel channel = file.getChannel();
                FileLock lock = channel.lock();
                Writer writer = new SSTWriteWithBuffer(channel);
        ) {
            System.out.println("commence");
//            channel.force(true);
            header.write(writer);
            System.out.println("written header");
            for (Map.Entry<byte[], ValueUnit> entry : map.entrySet()) {
                pointers.add(writer.position());

                // bloom
                filter.put(entry.getKey());
                MiddleBlock.writeMiddleBlock(writer, entry.getKey(), entry.getValue());
            }
            long bs = channel.position();
            MiddleBlock.writePointers(writer, pointers);
            System.out.println("written pointers");
            filter.writeTo(writer);
            header.writeBS(writer, bs);
            header.close(); // important to close
            System.out.println("finish written");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        String realFileName = table.getNewSST(Level.LEVEL_ZERO);
        Util.requireTrue(new File(tempFileName).renameTo(new File(realFileName)), "unable to rename files");
        var sstInfo = new SSTInfo(realFileName, header, pointers, filter, SSTFileHelper.getSparseBinarySearch(pointers, map));
        table.addSST(Level.LEVEL_ZERO, sstInfo);
        // debug
        b = System.nanoTime();
        System.out.println("took ="+(b - a) + " nano for level="+Level.LEVEL_ZERO+" sst to write");
        //compaction.compactionMaybe();
    }

    public byte[] search(byte[] key) throws Exception {
        for (Level value : Level.values()) {
            List<SSTInfo> SSTInfoList = table.getLevelFileList(value);
            for (SSTInfo sstInfo : SSTInfoList) {

                long c,d;
                c = System.nanoTime();
                ValueUnit found = findValue(sstInfo, key);
                d = System.nanoTime();
                if (found != null) {
                    if (found.getIsDelete() == ValueUnit.DELETE){
                        return null;
                    }
                    System.out.println("took " + ((d-c)/1000_000_000.0));
                    return found.getValue();
                }
            }
        }
        return null;
    }

    private ValueUnit findValue(SSTInfo sstInfo, byte[] key) {
        if (!sstInfo.getBloomFilter().mightContain(key)) {
            return null;
        }
        // bound check
        if (DBComparator.byteArrayComparator.compare(key, sstInfo.getHeader().getSmallestKey()) < 0) {
            return null;
        }
        if (DBComparator.byteArrayComparator.compare(key, sstInfo.getHeader().getLargestKey()) > 0) {
            return null;
        }
        try(
                RandomAccessFile randomAccessFile = new RandomAccessFile(sstInfo.getFileName(), "r");
                FileChannel channel = randomAccessFile.getChannel();
                Reader reader = new SSTReaderWithBuffer(channel);
        ) {
            Function<Long, KeyUnit> keyRetriever = position -> {
                System.out.println("position="+position);
                KeyUnit keyUnit = sstInfo.getSparseBinarySearch().get(position);
                if (keyUnit == null) {
                    System.out.println("reading key from file");
                    keyUnit = MiddleBlock.getKeyUnit(reader, position);
                } else {
                    System.out.println("reading key sparse");
                }
                return keyUnit;
            };
            BiFunction<Long, KeyUnit, ValueUnit> valueRetriever = (position, keyUnit) -> {
                if (keyUnit.getIsDelete() == KeyUnit.DELETE) {
                    return null;
                }
                try {
                    System.out.println("find key, key string=" + new String(Snappy.uncompress(keyUnit.getKey())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                byte[] valueUnit = MiddleBlock.getValueUnit(reader, position, keyUnit);
                return new ValueUnit(valueUnit, keyUnit.getIsDelete());
            };
            return binarySearch(sstInfo, keyRetriever, key, valueRetriever);
        } catch (Exception e) {
            System.out.println("Moye moye while searching");
            throw new RuntimeException(e);
        }
    }

    private ValueUnit binarySearch(SSTInfo sstInfo, Function<Long, KeyUnit> keyRetriever, byte[] key, BiFunction<Long, KeyUnit, ValueUnit> valueRetriever) {
        int l = 0, h = sstInfo.getPointers().size() -1;
        while(l <= h) {
            int mid = (l + h) >>> 1;
            long position = sstInfo.getPointers().get(mid);
            KeyUnit keyUnit = keyRetriever.apply(position);
            int compare = DBComparator.byteArrayComparator.compare(keyUnit.getKey(), key);
            if (compare < 0){
                l = mid + 1;
            }
            else if (compare > 0) {
                h = mid - 1;
            }
            else {
                return valueRetriever.apply(position, keyUnit);
            }
        }
        return null;
    }
}


// todo
// change the file naming for sst, too much computation for Instant.now()......
//todo
// writing to a file in a batch, rather then every kv write multiple kv at a time.

// todo
//check what if the sst only has one kv which is big, will all algo work fine?

// todo |  performance improvement -> we can store BS at the end and if we want to retrieve then (Size - long.bytes) and read the bs.
// but for this to work we need to check if bloom filter only reads specific bytes, if yes then great else what will happen is that
// while reading bloom filter it will read the BS as well.


/**
 * Todo Performance improvement
 * File a bloom filter library which can be managed by us. so the we can store it effeciently as well as we know what we are storing
 * not only we will be able to remove outputStream but also get to have more control over where we store and when to retrieve
 * Find library and compare the performance
 */