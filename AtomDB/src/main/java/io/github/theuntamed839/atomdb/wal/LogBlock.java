package io.github.theuntamed839.atomdb.wal;

import io.github.theuntamed839.atomdb.constants.Operations;
import io.github.theuntamed839.atomdb.db.KVUnit;

public record LogBlock(Operations operation, KVUnit kvUnit) {
}