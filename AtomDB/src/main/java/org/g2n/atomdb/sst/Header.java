package org.g2n.atomdb.sst;

import org.g2n.atomdb.Checksum.CheckSumStatic;
import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.sstIo.ReaderInterface;
import org.g2n.atomdb.sstIo.PrimitiveWriter;
import org.g2n.atomdb.util.SizeOf;
import org.g2n.atomdb.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *  closing of header is important as it provided easy debugging and safety
 */

public class Header implements AutoCloseable{
    private static final long BS_POSITION = 2;
    private static final int HEADER_POSITION = 0;
    private final byte versionId;
    private String fileName;
    private int entries = Integer.MIN_VALUE;
    private final byte[] sKey;
    private final byte[] lKey;
    private long binarySearchLocation = Long.MIN_VALUE;
    private final Level level;
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

    public Header(byte versionId, Level level, long bs, int entries, byte[] sKey, byte[] lKey, long checksum) {
        this.versionId = versionId;
        this.level = level;
        this.binarySearchLocation = bs;
        this.entries = entries;
        this.sKey = sKey;
        this.lKey = lKey;
        validation(checksum);
    }

    public Header(byte[] firstKey, byte[] lastKey, int size, byte sstVersion, Level levelZero, String tempFileName) {
        this.versionId = sstVersion;
        this.entries = size;
        this.sKey = firstKey;
        this.lKey = lastKey;
        this.level = levelZero;
        this.fileName = tempFileName;
        this.isWritten = true; // this is used for safety of writing, since this constructor is used by reading there is writing required. that's why this is set to true
    }

    private void validation(long checksum) {
        if (checksum != getKeysChecksum()) {
            throw new RuntimeException("Mismatch of checksum");
        }
        Util.requireTrue(
                DBComparator.byteArrayComparator.compare(
                sKey, lKey
                ) < 0, "found SmallVal>LargerVAl " + this);
    }

    @Override
    public String toString() {
        return "org.g2n.atomdb.sst.Header{" +
                "versionId=" + versionId +
                ", entries=" + entries +
                ", sKey=" + new String(sKey) +
                ", lKey=" + new String(lKey) +
                ", binarySearchLocation=" + binarySearchLocation +
                ", org.g2n.atomdb.Level.org.g2n.atomdb.Level=" + level +
                ", FileName=" + fileName +
                '}';
    }

    public long getVersionId() {
        return versionId;
    }

    public Level getLevel() {
        return level;
    }

    public int getEntries() {
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

    public static Header getHeader(ReaderInterface sstReader) throws Exception {
        var byteBuffer = sstReader.readSize(new byte[18], HEADER_POSITION, 18);
        byte verId = byteBuffer.get();
        byte levelID = byteBuffer.get();
        long bs = byteBuffer.getLong();
        int entries = byteBuffer.getInt();
        int nextBlockLength = byteBuffer.getInt();

        byteBuffer = sstReader.readSize(new byte[(int) nextBlockLength], nextBlockLength);

        // todo key and value sizes are int
        int sLength = byteBuffer.getInt();
        byte[] sKey = new byte[sLength];
        byteBuffer.get(sKey);

        int lLength = byteBuffer.getInt();
        byte[] lKey = new byte[lLength];
        byteBuffer.get(lKey);

        long checkSum = byteBuffer.getLong();

        return new Header(verId, Level.fromID((int) levelID), bs, entries, sKey, lKey, checkSum);
    }

    public void write(PrimitiveWriter writer) throws Exception{
//        VID | LEV | BS | EN | Block_LEN | [ SK_LEN | SK | LK_LEN | LK | CH ]
        // todo what about size exceeding bytebuffer length as well as the mappedBuffer length
        writer.putByte(versionId)
                .putByte(Level.toID(level))
                .putLong(binarySearchLocation)
                .putInt(entries)
                .putInt(SizeOf.IntLength + sKey.length + SizeOf.IntLength + lKey.length + SizeOf.LongLength)
                .putInt(sKey.length)
                .putBytes(sKey)
                .putInt(lKey.length)
                .putBytes(lKey)
                .putLong(getKeysChecksum());
        isWritten = true;
    }

    private long getKeysChecksum() {
        return CheckSumStatic.compute(sKey, lKey);
    }

    public void writeBS(FileChannel channel, ByteBuffer byteBuffer, long binarySearchLocation) throws IOException {
        Util.requireEquals(this.binarySearchLocation, Long.MIN_VALUE, "overwriting of binary org.g2n.atomdb.search position, fileToWrite="+ fileName);
        this.binarySearchLocation = binarySearchLocation;
        byteBuffer.clear();
        byteBuffer.putLong(binarySearchLocation)
                  .flip();
        channel.write(byteBuffer, BS_POSITION);
    }

    public void writeBS(PrimitiveWriter writer, long binarySearchLocation) throws IOException {
        Util.requireEquals(this.binarySearchLocation, Long.MIN_VALUE, "overwriting of binary org.g2n.atomdb.search position, fileToWrite="+ fileName);
        this.binarySearchLocation = binarySearchLocation;
//        writer.writeAtPositionInIsolation(BS_POSITION, binarySearchLocation);
    }


    @Override
    public void close() throws Exception {
        Util.requireNotEquals(this.entries,  Long.MIN_VALUE, "overwriting of entries, fileToWrite=" + fileName);
        Util.requireNotEquals(this.binarySearchLocation, Long.MIN_VALUE, "overwriting of binary org.g2n.atomdb.search position, fileToWrite="+ fileName);
        Util.requireTrue(isWritten, "header purpose is not served as the header is not written");
    }
}
