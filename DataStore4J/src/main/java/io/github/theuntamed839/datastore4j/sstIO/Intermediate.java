package io.github.theuntamed839.datastore4j.sstIO;

import com.google.common.hash.BloomFilter;
import io.github.theuntamed839.datastore4j.compaction.PointerList;

import java.nio.file.Path;

public record Intermediate(Path path, SSTHeader sstHeader, PointerList pointers, BloomFilter<byte[]> filter) {}
