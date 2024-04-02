package Compaction;

import com.google.common.base.Preconditions;
import sstIo.ChannelBackedWriter;

import java.util.*;

public class PointerList {
    private final List<Pointer> pointers;
    private Pointer previous;

    public PointerList(int numberOfEntries) {
        this.pointers = new ArrayList<>(numberOfEntries);
    }

    public void add(Pointer pointer) {
        Preconditions.checkArgument(previous != null && previous.compareTo(pointer) < 0);
        previous = pointer;
        pointers.add(pointer);
    }

    public Pointer get(int index) {
        return pointers.get(index);
    }

    public void storeAsBytes(ChannelBackedWriter writer) {
        pointers.forEach(each -> each.storeAsBytes(writer));
    }

    public int size() {
        return pointers.size();
    }
}
