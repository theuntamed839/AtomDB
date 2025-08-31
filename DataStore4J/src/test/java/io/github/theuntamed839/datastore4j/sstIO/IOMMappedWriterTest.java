package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.file.Path;

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