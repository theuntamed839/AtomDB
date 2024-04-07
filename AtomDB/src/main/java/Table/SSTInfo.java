package Table;

import Compaction.PointerList;
import Level.Level;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import sstIo.SSTHeader;
import sstIo.SSTKeyRange;

import java.io.File;
import java.util.Objects;

public class SSTInfo extends SSTHeader implements AutoCloseable, Comparable<SSTInfo>{

    private final File sst;
    private final PointerList pointers;
    private final BloomFilter<byte[]> filter;
    private final int number;
    private final SSTKeyRange sstKeyRange;

    public SSTInfo(File sst, SSTHeader header, PointerList pointers, BloomFilter<byte[]> filter) {
        super(header);
        Preconditions.checkArgument(sst.exists());
        this.number = Integer.parseInt(sst.getName().trim().split("_")[1].trim().replace(".sst", ""));
        System.out.println(sst.getName());
        System.out.println(sst.getName().charAt(0) - 48);
        System.out.println(Level.fromID(sst.getName().charAt(0) - 48));
        System.out.println(getLevel());
        Preconditions.checkArgument(Level.fromID(sst.getName().charAt(0) - 48).equals(getLevel()));
        this.sst = sst;
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
            return Integer.compare(this.number, sstInfo.number);
        }
        return  Byte.compare(this.getLevel().value(), sstInfo.getLevel().value());
    }

    public static File newFile(String filePath, Level level, int number) {
        return new File(filePath + File.separator +
                level.value() + "_" + number + ".sst");
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
        int result = sst != null ? sst.hashCode() : 0;
        result = 31 * result + (pointers != null ? pointers.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }
}
