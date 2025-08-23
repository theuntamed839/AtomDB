package io.github.theuntamed839.atomdb.sstIO;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface IOWriterFactory {
    IOWriter create(Path path, long fileSize) throws IOException;
}
