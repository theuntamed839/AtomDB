package org.g2n.atomdb.Table;

import org.g2n.atomdb.Compaction.PointerList;
import org.g2n.atomdb.Level.Level;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import org.g2n.atomdb.sstIo.SSTHeader;
import org.g2n.atomdb.sstIo.SSTKeyRange;

import java.io.File;

public class SSTInfo extends SSTHeader implements Comparable<SSTInfo> {
    private final File sst;
    private final PointerList pointers;
    private final BloomFilter<byte[]> filter;
    private final int number;
    private final SSTKeyRange sstKeyRange;
    private final int sstHashCode;


    public SSTInfo(File sst, SSTHeader header, PointerList pointers, BloomFilter<byte[]> filter) {
        super(header);
        Preconditions.checkArgument(sst.exists());
        this.number = Integer.parseInt(sst.getName().trim().split("_")[1].trim().replace(".org.g2n.atomdb.sst", ""));
        Preconditions.checkArgument(Level.fromID(sst.getName().charAt(0) - 48).equals(getLevel()));
        this.sst = sst;
        this.sstHashCode = sst.getAbsolutePath().hashCode();
        this.pointers = pointers;
        this.filter = filter;
        this.sstKeyRange = new SSTKeyRange(pointers.getFirst().key(), pointers.getLast().key());
    }

    public SSTKeyRange getSstKeyRange() {
        return sstKeyRange;
    }

    public File getSst() {
        return sst;
    }

    public PointerList getPointers() {
        return pointers;
    }

    public boolean mightContainElement(byte[] key) {
        return filter.mightContain(key);
    }

    @Override
    public int compareTo(SSTInfo sstInfo) {
        if (this.getLevel().equals(sstInfo.getLevel())) {
            return Integer.compare(sstInfo.number, this.number); // newer fileToWrite will have greater number, and they should come first.
        }
        return  Byte.compare(this.getLevel().value(), sstInfo.getLevel().value());
    }

    public static File newFile(String filePath, Level level, long number) {
        return new File(filePath + File.separator +
                level.value() + "_" + number + ".org.g2n.atomdb.sst");
    }

    public int getFileTorsoSize() {
        return (int) (getPointersPosition() - pointers.getLast().position());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SSTInfo sstInfo = (SSTInfo) o;
        return sst.getAbsolutePath().equals(sstInfo.sst.getAbsolutePath());
    }

    @Override
    public int hashCode() {
        return sstHashCode;
    }

    @Override
    public String toString() {
        return "SSTInfo{" +
                "sst=" + sst +
                ", pointers=" + pointers.toString() +
                ", filter=" + filter +
                ", number=" + number +
                ", sstKeyRange=" + sstKeyRange.toString() +
                ", sstHashCode=" + sstHashCode +
                ", SSTHeader=" + super.toString() +
                '}';
    }
}
