package Compaction;

import db.DBComparator;
import sst.Header;
import sst.MiddleBlock;
import sst.ValueUnit;
import util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class Helper implements Comparable<Helper>, Iterator<Map.Entry<byte[], ValueUnit>>, AutoCloseable {
    private final FileChannel channel;
    private final ByteBuffer byteBuffer;
    private final List<Long> pointers;
    private final Header header;
    private int pointerIndex;
    private final long fileRankInLevel;
    private final short fileLevel;
    private boolean startedIter = false;
    public Helper(String file) throws Exception {
        this.channel = new FileInputStream(file).getChannel();
        this.byteBuffer = ByteBuffer.allocate(4096);
        System.out.printf("helper file="+file);
        // file rank and level
        String[] pieces = file.trim().split(File.separator);
        pieces  = pieces[pieces.length - 1].trim().split("_");
        this.fileLevel = Short.parseShort(pieces[0].trim());
        this.fileRankInLevel = Long.parseLong(pieces[1].replace(".sst", ""));

        this.header = Header.getHeader(file, channel, byteBuffer);
        // todo can be retrived from cache
        this.pointers = MiddleBlock.readPointers(channel,
                byteBuffer,
                header.getBinarySearchLocation(),
                new ArrayList<>((int) header.getEntries()),
                header.getEntries());

        Util.requireEquals(header.getEntries(), pointers.size(), "pointers and entries from header doesn't match");
    }

    public long getEntries() {
        return header.getEntries();
    }

    public byte[] getSmallestKey() {
        return header.getSmallestKey();
    }

    public byte[] getlargestKey() {
        return header.getLargestKey();
    }

    @Override
    public int compareTo(Helper provided) {
        if (provided.hasNext()) {
            if (this.hasNext()) {
                int val = 0;

                try {
                    val = DBComparator.byteArrayComparator.compare(
                            this.peekKey(), provided.peekKey());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (val == 0) {
                    if (this.fileLevel == provided.fileLevel) {
                        // purposely
                        return Long.compare(provided.fileRankInLevel, this.fileRankInLevel);
                    }
                    return Short.compare(this.fileLevel, provided.fileLevel);
                }
                return val;
            } else {
                return 1; // giving more value for provided
            }
        } else {
            return this.hasNext() ? -1 : 0;
        }
    }


    public void iterate() {
        Util.requireFalse(startedIter, "iterator started in between the current run");
        startedIter = true;
        pointerIndex = 0;
    }

    public byte[] peekKey() throws IOException {
        if (hasNext())
            return MiddleBlock.readKey(channel, byteBuffer, pointers.get(pointerIndex));
        return null;
    }

    @Override
    public boolean hasNext() {
        return pointerIndex < pointers.size();
    }

    @Override
    public Map.Entry<byte[], ValueUnit> next() {
        try {
            long bloomLine = header.getBinarySearchLocation() + Long.BYTES * header.getEntries();
            if (channel.position() == bloomLine) {
                throw new RuntimeException("Bloom filter line access");
            }

            return MiddleBlock.readKeyValue(channel,
                    byteBuffer,
                    pointers.get(pointerIndex++));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        channel.close();
        header.close();
        if (startedIter) {
            Util.requireEquals(pointerIndex, pointers.size(), " iterator mistake");
        }
    }
}
