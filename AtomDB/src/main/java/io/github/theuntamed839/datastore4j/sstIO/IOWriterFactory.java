package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface IOWriterFactory {
    IOWriter create(Path path, long fileSize) throws IOException;
}
