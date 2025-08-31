package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface IOReaderFactory {
    IOReader create(Path path) throws IOException;
}
