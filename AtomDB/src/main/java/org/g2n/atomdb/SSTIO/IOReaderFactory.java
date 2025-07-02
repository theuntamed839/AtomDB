package org.g2n.atomdb.SSTIO;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface IOReaderFactory {
    IOReader create(Path path) throws IOException;
}
