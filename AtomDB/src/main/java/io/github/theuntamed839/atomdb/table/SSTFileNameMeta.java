package io.github.theuntamed839.atomdb.table;

import io.github.theuntamed839.atomdb.level.Level;

import java.nio.file.Path;

public record SSTFileNameMeta(Path path, Level level, long seq) { }
