package org.g2n.atomdb.SSTIO;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface IOWriterFactory {
    IOWriter create(Path path, long fileSize) throws IOException;
}
