package org.g2n.atomdb.SSTIO;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IOMMappedWriterTest extends AbstractIOWriterTest {

    @Override
    protected IOWriter createWriter(Path path, long fileSize) throws IOException {
        return new IOMMappedWriter(path, fileSize);
    }

    @Override
    protected boolean requiresNativeDiskAccess() {
        return true;
    }
}