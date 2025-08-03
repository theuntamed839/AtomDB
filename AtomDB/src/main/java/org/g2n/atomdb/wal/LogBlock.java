package org.g2n.atomdb.wal;

import org.g2n.atomdb.constants.Operations;
import org.g2n.atomdb.db.KVUnit;

record LogBlock(Operations operation, KVUnit kvUnit) {
}