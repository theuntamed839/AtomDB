package org.g2n.atomdb.sstIo;

import com.google.common.hash.BloomFilter;
import org.g2n.atomdb.Compaction.PointerList;

import java.nio.file.Path;

record Intermediate(Path path, SSTHeader sstHeader, PointerList pointers, BloomFilter<byte[]> filter) {}
