package Compaction;


import Constants.DBConstant;
import Level.Level;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.DBComparator;
import sst.Header;
import sst.MiddleBlock;
import sst.ValueUnit;
import util.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * todo
 * deletion is limited
 * the key and the deletion marker doesn't get removed fully
 *
 * Solution
 * will coming across a key which is deleted we can do a search in the down levels
 * and level files in that same level and check if the key is present.
 * if present then keep the marker and if not then delete it here
 * further this can be improved with bloom
 */

public class Compactor {
    private final List<String> files;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
    private final File compactionFile;
    private final Level level;

    public Compactor(List<String> files, File compactionFile, Level level) {
        Objects.requireNonNull(files);Objects.requireNonNull(compactionFile);
        this.level = level;
        this.files = files;
        this.compactionFile = compactionFile;
    }

    //todo breakDown compact method
    public void compact() {

        // debug
        long a, b;
        a = System.nanoTime();

        try(FileOutputStream outputStream = new FileOutputStream(compactionFile);
            FileChannel channel = outputStream.getChannel();
            ) {
            List<Helper> helperList = getHelperList(files);
            byte[][] firstAndLast = getSmallestANDLargest(helperList);
            var header = new Header(DBConstant.SST_VERSION,
                    firstAndLast[0],
                    firstAndLast[1],
                    level.next(),
                    compactionFile.toPath().toString());
            header.writeHeader(channel, byteBuffer);

            PriorityQueue<Helper> qu = new PriorityQueue<>(helperList);
            qu.forEach(Helper::iterate);

            BloomFilter<byte[]> filter = BloomFilter.create(
                    Funnels.byteArrayFunnel(),
                    helperList.stream().mapToLong(e -> e.getEntries()).sum(),
                    0.01);

            List<Long> pointers = new ArrayList<>();
            long numberOfEntries = 0;
            byte[] previousKey = null;
            while (!qu.isEmpty()) {
                Helper helper = qu.remove();

                Map.Entry<byte[], ValueUnit> next = helper.next();

                if (Arrays.compare(previousKey, next.getKey()) != 0) {
                    pointers.add(channel.position());

                    // bloom
                    filter.put(next.getKey());

                    numberOfEntries++;
                    MiddleBlock.writeBlock(channel, byteBuffer, next);
                }

                previousKey = next.getKey();

                if (helper.hasNext()) {
                    qu.add(helper);
                } else {
                    helper.close();
                }
            }
            long bs = channel.position();

            MiddleBlock.writePointers(channel, byteBuffer, pointers);

            // bloom
            MiddleBlock.writeBloom(outputStream, filter);

            header.writeBS(channel, byteBuffer, bs);
            Util.requireEquals(pointers.size(), numberOfEntries, "entry number misMatch with arrayList");
            header.writeEntries(channel, byteBuffer, pointers.size());
            header.close();
        } catch (Exception e) {
            throw new RuntimeException("while compacting_file=" + compactionFile, e);
        }

        // debug
        b = System.nanoTime();
        System.out.println("took ="+(b - a) + " nano for level="+level+" sst to create from compact");
    }


    private byte[][] getSmallestANDLargest(List<Helper> helperList) {
        byte[] smallest = helperList.get(0).getSmallestKey();
        byte[] largest = helperList.get(0).getlargestKey();

        for (Helper helper : helperList) {
            if (DBComparator.byteArrayComparator
                    .compare(smallest, helper.getSmallestKey()) > 0) {
                smallest = helper.getSmallestKey();
            }
            if (DBComparator.byteArrayComparator
                    .compare(largest, helper.getlargestKey()) < 0) {
                largest = helper.getlargestKey();
            }
        }

        return new byte[][] {
                smallest, largest
        };
    }

    private List<Helper> getHelperList(List<String> files) throws Exception {
            List<Helper> list = new ArrayList<>(files.size());
            for (String file : files) {
                list.add(
                        new Helper(file)
                );
            }
            return list;
    }
}
