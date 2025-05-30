package org.g2n.atomdb.Table;

import org.g2n.atomdb.Level.Level;

import java.nio.file.Path;

public record SSTFileNameMeta(Path path, Level level, long seq) { }
