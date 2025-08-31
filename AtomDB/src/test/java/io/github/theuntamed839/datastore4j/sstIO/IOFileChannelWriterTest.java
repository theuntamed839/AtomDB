package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.file.Path;

class IOFileChannelWriterTest extends AbstractIOWriterTest {

    @Override
    protected IOWriter createWriter(Path path, long IGNORED_FILE_SIZE) throws IOException {
        return new IOFileChannelWriter(path, IGNORED_FILE_SIZE);
    }

    @Override
    protected boolean requiresNativeDiskAccess() {
        return false;
    }
}