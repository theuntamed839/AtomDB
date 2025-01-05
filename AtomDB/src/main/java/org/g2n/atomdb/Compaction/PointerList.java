package org.g2n.atomdb.Compaction;

import com.google.common.base.Preconditions;
import org.g2n.atomdb.db.ExpandingByteBuffer;
import org.g2n.atomdb.sstIo.MMappedReader;
import org.g2n.atomdb.sstIo.ChannelBackedWriter;

import java.util.*;

public class PointerList {
    private final List<Pointer> pointers;
    private Pointer previous;

    public PointerList(int numberOfEntries) {
        this.pointers = new ArrayList<>(numberOfEntries);
    }

    public PointerList(){
        this.pointers = new ArrayList<>(5000);
    }


    public void add(Pointer pointer) {
        if (previous !=null ){
            Preconditions.checkArgument(previous.compareTo(pointer) <= 0);
        }
        previous = pointer;
        pointers.add(pointer);
    }

    public Pointer get(int index) {
        return pointers.get(index);
    }

    public Pointer getFirst() { return pointers.getFirst();}
    public Pointer getLast() {return  pointers.getLast();}

    public void storeAsBytes(ChannelBackedWriter writer) {
        pointers.forEach(each -> each.storeAsBytes(writer));
    }

    public void storeAsBytes(ExpandingByteBuffer writer) {
        pointers.forEach(each -> each.storeAsBytes(writer));
    }

    public static PointerList getPointerList(MMappedReader reader, int entries) {
        var pointers = new PointerList(entries);
        for (int i = 0; i < entries; i++) {
            pointers.add(Pointer.getPointer(reader));
        }
        return pointers;
    }

    public int size() {
        return pointers.size();
    }

    @Override
    public String toString() {
        return "PointerList{" +
                "firstPointer=" + pointers.getFirst().toString() +
                "secondLast=" + pointers.get(pointers.size() - 2).toString() +
                ", LastPointer=" + pointers.getLast().toString() +
                '}';
    }

    public List<Pointer> getList() {
        return Collections.unmodifiableList(pointers);
    }
}
