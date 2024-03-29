package Compaction;

import Mem.ImmutableMem;
import Mem.SkipListMemtable;
import sstIo.SSTReader;

import java.util.Collections;

public class Compactor {

    public void persistLevelFile(ImmutableMem memtable) {
        new SSTReader(memtable);
    }
}
