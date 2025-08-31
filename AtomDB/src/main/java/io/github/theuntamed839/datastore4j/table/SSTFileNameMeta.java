package io.github.theuntamed839.datastore4j.table;

import io.github.theuntamed839.datastore4j.level.Level;

import java.nio.file.Path;

public record SSTFileNameMeta(Path path, Level level, long seq) { }
