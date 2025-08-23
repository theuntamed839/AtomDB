package io.github.theuntamed839.atomdb.sstIO;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface IOReaderFactory {
    IOReader create(Path path) throws IOException;
}
