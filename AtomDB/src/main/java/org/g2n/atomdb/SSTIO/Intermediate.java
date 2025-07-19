package org.g2n.atomdb.SSTIO;

import com.google.common.hash.BloomFilter;
import org.g2n.atomdb.Compaction.PointerList;

import java.nio.file.Path;

public record Intermediate(Path path, SSTHeader sstHeader, PointerList pointers, BloomFilter<byte[]> filter) {}
