package org.g2n.atomdb.table;

import org.g2n.atomdb.level.Level;

import java.nio.file.Path;

public record SSTFileNameMeta(Path path, Level level, long seq) { }
