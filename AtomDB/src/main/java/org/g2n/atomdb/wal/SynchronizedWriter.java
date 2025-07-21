package org.g2n.atomdb.wal;

public sealed interface SynchronizedWriter extends Writer permits SynchronizedFileChannelWriter {
}
