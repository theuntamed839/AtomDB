package sst;

import Checksum.CheckSum;
import Level.Level;
import com.google.common.primitives.Longs;
import db.DBComparator;
import util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedMap;

/**
 *  closing of header is important as it provided easy debugging and safety
 */

public class Header implements AutoCloseable{
    private static final int BS_POSITION = 16;
    private static final int ENTRIES_POSITION = 24;
    private static final int HEADER_POSITION = 0;
    private final long versionId;
    private long entries = Long.MIN_VALUE;
    private final byte[] sKey;
    private final byte[] lKey;
    private long binarySearchLocation = Long.MIN_VALUE;
    private final Level level;
    private final String fileName;
    /**
     *  isWritten
     *  this bool is there for the safety for writing
     *  header is created with purpose
     *  header is created when reading then isWritten is marked as true
     *  while its actual reason being here is for writing
     *  that is if header is created then it needs to be written
     *  if this is written then it is set to true else it is already marked as false
     *
     *  when header is closed this bool is checked along with entries and binarySearchLocation to Long.MIN_VALUE
     *  if bool is set to false or entries and binarySearchLocation is set to Long.MIN then the purpose of header is not servered
     *  to which a exception is thrown
     */
    private boolean isWritten = false;
    public Header(byte[] firstKey, byte[] lastKey, long numberOfEntries, long verID, Level level, String fileName) {
        this.entries = numberOfEntries;
        this.versionId = verID;
        sKey = firstKey;
        lKey = lastKey;
        this.level = level;
        this.fileName = fileName;
        validation();
    }

    private Header(long versionId,
                  long entries,
                  byte[] sKey,
                  byte[] lKey,
                  long binarySearchLocation,
                  Level level,
                  String fileName) {
        this.versionId = versionId;
        this.entries = entries;
        this.sKey = sKey;
        this.lKey = lKey;
        this.binarySearchLocation = binarySearchLocation;
        this.level = level;
        this.fileName = fileName;
        this.isWritten = true; // this is used for safety of writing, since this constructor is used by reading there is writing required. that's why this is set to true
        validation();
    }

    public Header(long versionId,
                  byte[] sKey,
                  byte[] lKey,
                  Level level,
                  String fileName) {
        this.versionId = versionId;
        this.sKey = sKey;
        this.lKey = lKey;
        this.level = level;
        this.fileName = fileName;
        validation();
    }

    private void validation() {
        Util.requireTrue(
                DBComparator.byteArrayComparator.compare(
                sKey, lKey
                ) < 0, "found SmallVal>LargerVAl " + this);
    }

    @Override
    public String toString() {
        return "sst.Header{" +
                "versionId=" + versionId +
                ", entries=" + entries +
                ", sKey=" + new String(sKey) +
                ", lKey=" + new String(lKey) +
                ", binarySearchLocation=" + binarySearchLocation +
                ", Level.Level=" + level +
                ", FileName=" + fileName +
                '}';
    }

    public long getVersionId() {
        return versionId;
    }

    public Level getLevel() {
        return level;
    }

    public long getEntries() {
        return entries;
    }

    public byte[] getSmallestKey() {
        return sKey;
    }

    public byte[] getLargestKey() {
        return lKey;
    }

    public long getBinarySearchLocation() {
        return binarySearchLocation;
    }

    public static Header getHeader(String fileName, FileChannel channel, ByteBuffer byteBuffer) throws Exception {
        byteBuffer.clear().limit(Long.BYTES * 5);
        channel.position(HEADER_POSITION);
        channel.read(byteBuffer);
        byteBuffer.flip();
        long verId = byteBuffer.getLong();
//        System.out.println(verId);
        long levelID = byteBuffer.getLong();
//        System.out.println(lev);
        long bs = byteBuffer.getLong();
//        System.out.println(bs);
        long entries = byteBuffer.getLong();
//        System.out.println(entries);
        int sLength = (int) byteBuffer.getLong();
//        System.out.println(sLength);
        byte[] sKey = new byte[sLength];
        byteBuffer.clear().limit(sLength);
        channel.read(byteBuffer);
        byteBuffer.flip();
        byteBuffer.get(sKey);

        byteBuffer.clear().limit(Longs.BYTES * 2);
        channel.read(byteBuffer);
        byteBuffer.flip();
        long sCheckSum = byteBuffer.getLong();
        int lLength = (int) byteBuffer.getLong();
        byte[] lKey = new byte[lLength];
        byteBuffer.clear().limit(lLength);
        channel.read(byteBuffer);
        byteBuffer.flip();
        byteBuffer.get(lKey);

        byteBuffer.clear().limit(Longs.BYTES);
        channel.read(byteBuffer);
        byteBuffer.flip();
        long lCheckSum = byteBuffer.getLong();
        if (lCheckSum != CheckSum.compute(lKey) ||
                sCheckSum != CheckSum.compute(sKey)) {
            throw new Exception("wrong checksum for smallest and largest");
        }
        return new Header(verId, entries, sKey, lKey, bs, Level.fromID((int) levelID), fileName);
    }

    public void writeHeader(FileChannel channel, ByteBuffer byteBuffer) throws Exception{
        byteBuffer.clear();
        byteBuffer.putLong(versionId)
                .putLong(Level.toID(level))
                .putLong(binarySearchLocation)
                .putLong(entries)
                .putLong(sKey.length)
                .put(sKey)
                .putLong(CheckSum.compute(sKey))
                .putLong(lKey.length)
                .put(lKey)
                .putLong(CheckSum.compute(lKey))
                .flip();
        if (channel.position() != HEADER_POSITION) {
            channel.position(HEADER_POSITION); // moved to 0 position
        }
        channel.write(byteBuffer);
        isWritten = true;
    }

    public void fillBufferWithHeaderData(ByteBuffer byteBuffer) {
        // todo working with assumption that buffer has enough space for these data to be filled.
        byteBuffer.putLong(versionId)
                .putLong(Level.toID(level))
                .putLong(binarySearchLocation)
                .putLong(entries)
                .putLong(sKey.length)
                .put(sKey)
                .putLong(CheckSum.compute(sKey))
                .putLong(lKey.length)
                .put(lKey)
                .putLong(CheckSum.compute(lKey));
    }

    public void writeBS(FileChannel channel, ByteBuffer byteBuffer, long binarySearchLocation) throws IOException {
        Util.requireEquals(this.binarySearchLocation, Long.MIN_VALUE, "overwriting of binary search position, file="+ fileName);
        this.binarySearchLocation = binarySearchLocation;
        byteBuffer.clear();
        byteBuffer.putLong(binarySearchLocation)
                  .flip();
        channel.write(byteBuffer, BS_POSITION);
    }

    public void writeEntries(FileChannel channel, ByteBuffer byteBuffer, long numberOfEntries) throws IOException {
        Util.requireEquals(this.entries,  Long.MIN_VALUE, "overwriting of entries, file=" + fileName);
        this.entries = numberOfEntries;
        byteBuffer.clear();
        byteBuffer.putLong(numberOfEntries)
                .flip();
        channel.write(byteBuffer, ENTRIES_POSITION);
    }


    @Override
    public void close() throws Exception {
        Util.requireNotEquals(this.entries,  Long.MIN_VALUE, "overwriting of entries, file=" + fileName);
        Util.requireNotEquals(this.binarySearchLocation, Long.MIN_VALUE, "overwriting of binary search position, file="+ fileName);
        Util.requireTrue(isWritten, "header purpose is not served as the header is not written");
    }
}
