package io.github.theuntamed839.datastore4j.wal;

import io.github.theuntamed839.datastore4j.constants.Operations;
import io.github.theuntamed839.datastore4j.db.KVUnit;

public record LogBlock(Operations operation, KVUnit kvUnit) {
}