package sst;

import Compaction.Compaction;
import Constants.DBConstant;
import Level.Level;
import Table.Table;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.DBComparator;
import db.DBOptions;
import util.SizeOf;
import util.Util;
import Table.Cache;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
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
 *              { KB_LEN | K | K_Checksum | MAR } * N times
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
    private ByteBuffer byteBuffer;
    private int currentBufferSize = 4 * SizeOf.MB;
    private Table table;
    // todo need to move cache to somewhere, maybe in table
    private Cache cache;

    //todo need a better place for Compaction
    private Compaction compaction;
    public SSTManager(File dbFolder) {
        this.dbFolder = dbFolder;
        this.table = new Table(dbFolder);
        byteBuffer = ByteBuffer.allocateDirect(currentBufferSize);
        this.cache = new Cache();
        this.compaction = new Compaction(dbFolder, table);
    }

//|verID|BS|NKeys|L|Skey|L|LKey|L|1Key|L|Value|...|1Pointer|2Pointer|...

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

        try(FileOutputStream outputStream = new FileOutputStream(tempFileName);
            FileChannel channel = outputStream.getChannel();) {

            header.writeHeader(channel, byteBuffer);

            List<Long> pointers = new ArrayList<>(map.size());
            for (Map.Entry<byte[], ValueUnit> entry : map.entrySet()) {
                pointers.add(channel.position());

                // bloom
                filter.put(entry.getKey());

                MiddleBlock.writeBlock(channel, byteBuffer, entry);
            }

            long bs = channel.position();
            MiddleBlock.writePointers(channel, byteBuffer, pointers);
            System.out.println("bloom writing position" + channel.position() + " bs position=" + bs);
            MiddleBlock.writeBloom(outputStream, filter);

            header.writeBS(channel, byteBuffer, bs);
            header.close(); // important to close
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String realFileName = table.getNewSST(Level.LEVEL_ZERO);
        Util.requireTrue(new File(tempFileName).renameTo(new File(realFileName)), "unable to rename files");
        table.addSST(Level.LEVEL_ZERO, realFileName);

        // debug
//        var vali = new Validate(new File(realFileName));
//        vali.isValid();

        // debug
        b = System.nanoTime();
        System.out.println("took ="+(b - a) + " nano for level="+Level.LEVEL_ZERO+" sst to write");

        compaction.compactionMaybe();
    }

    public void createSSTPerformant(SortedMap<byte[], ValueUnit> map) throws Exception {
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

        try(FileOutputStream outputStream = new FileOutputStream(tempFileName);
            FileChannel channel = outputStream.getChannel();
            FileLock lock = channel.lock()) {
            channel.force(true);

            header.writeHeader(channel, byteBuffer);

            List<Long> pointers = new ArrayList<>(map.size());
            byteBuffer.clear();
            for (Map.Entry<byte[], ValueUnit> entry : map.entrySet()) {
                pointers.add(channel.position() + byteBuffer.position());

                // bloom
                filter.put(entry.getKey());

                if (!MiddleBlock.fillMiddleBlockInBuffer(byteBuffer, entry.getKey(), entry.getValue())) {
                    byteBuffer.flip();
                    channel.write(byteBuffer);
                    byteBuffer.clear();
                    MiddleBlock.fillMiddleBlockInBuffer(byteBuffer, entry.getKey(), entry.getValue());
                }
            }
            byteBuffer.flip();
            channel.write(byteBuffer);
            byteBuffer.clear();

            long bs = channel.position();
            MiddleBlock.writePointers(channel, byteBuffer, pointers);

            MiddleBlock.writeBloom(outputStream, filter);

            header.writeBS(channel, byteBuffer, bs);
            header.close(); // important to close
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String realFileName = table.getNewSST(Level.LEVEL_ZERO);
        Util.requireTrue(new File(tempFileName).renameTo(new File(realFileName)), "unable to rename files");
        table.addSST(Level.LEVEL_ZERO, realFileName);

        // debug
//        var vali = new Validate(new File(realFileName));
//        vali.isValid();

        // debug
        b = System.nanoTime();
        System.out.println("took ="+(b - a) + " nano for level="+Level.LEVEL_ZERO+" sst to write");

        compaction.compactionMaybe();
    }

    public byte[] search(byte[] key) throws Exception {
        for (Level value : Level.values()) {
            List<String> list = table.getLevelList(value);
            for (String file : list) {

                // bloom
                if (!table.getBloom(file).mightContain(key)) {
                    continue;
                }

                Cache.CacheValue cacheValue = cache.get(file);
                ValueUnit found = null;
                if (cacheValue != null) {
                    found = tryBinarySearch(cacheValue, key, file);
                    // todo need to remove, strictly
//                    if (bytes != null) System.out.println("from cache sst");
                }else {
                    found = trySearch(file, key);
                }
                if (found != null) {
                    if (found.getIsDelete() == ValueUnit.DELETE){
                        return null;
                    }
                    return found.getValue();
                }
            }
        }
        return null;
    }

    private ValueUnit tryBinarySearch(Cache.CacheValue cacheValue, byte[] key, String file) throws Exception {
        // bound check
        if (Arrays.compare(key, cacheValue.smallKey()) < 0) {
            return null;
        }
        if (Arrays.compare(key, cacheValue.largeKey()) > 0) {
            return null;
        }

        try(FileInputStream inputStream = new FileInputStream(file);
            FileChannel channel = inputStream.getChannel();
        ) {
            Map.Entry<byte[], ValueUnit> entry = performBinarySearch(channel,
                    cacheValue.pointers(), key);
            if (entry == null) return null;
            else return entry.getValue();
        }
    }

    private ValueUnit trySearch(String file, byte[] key) throws Exception {
        // debug
        long a, b;
        a = System.nanoTime();

        Path filePath = Paths.get(file);
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            Header header = Header.getHeader(file, channel, byteBuffer);

            // bound check
            if (Arrays.compare(key, header.getSmallestKey()) < 0) {
                return null;
            }
            if (Arrays.compare(key, header.getLargestKey()) > 0) {
                return null;
            }

            // debug
            System.out.println(header);

            List<Long> pointers = MiddleBlock.readPointers(channel,
                    byteBuffer,
                    header.getBinarySearchLocation(),
                    new ArrayList<>((int) header.getEntries()),
                    header.getEntries());

            // todo need to remove, and place it neat fashion
            cache.put(file, header.getSmallestKey(), header.getLargestKey(), pointers);

            header.close();

            long c,d;
            c = System.nanoTime();
            Map.Entry<byte[], ValueUnit> entry = performBinarySearch(channel,
                    pointers, key);
            d = System.nanoTime();
            System.out.println("took ="+(d - c) + " nano for performing binary search a level="+header.getLevel()+" sst");

            // debug
            b = System.nanoTime();
            System.out.println("took ="+(b - a) + " nano for searching a level="+header.getLevel()+" sst");

            if (entry == null) {
                return null;
            } else {
                return entry.getValue();
            }
        }catch (Exception e) {
            throw new RuntimeException("while accessing file=" + file, e);
        }
    }

    // todo Map.Entry<byte[], ValueUnit> can be made in class
    private Map.Entry<byte[], ValueUnit> performBinarySearch(FileChannel channel, List<Long> pointers, byte[] key) throws Exception {
        int l = 0, h = pointers.size() -1;
        while(l <= h) {
            int mid = (l + h) >>> 1;
            Long offset = pointers.get(mid);
            byte[] foundKey = MiddleBlock.readKey(channel, byteBuffer, offset);
            int compare = DBComparator.byteArrayComparator.compare(foundKey, key);
            if (compare < 0)
                l = mid + 1;
            else if (compare > 0)
                h = mid - 1;
            else
                //todo can be improved
                return MiddleBlock.readKeyValue(channel, byteBuffer, offset);
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