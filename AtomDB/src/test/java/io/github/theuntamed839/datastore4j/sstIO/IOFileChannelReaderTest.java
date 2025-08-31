package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.file.Path;

public class IOFileChannelReaderTest extends AbstractIOReaderTest{

    @Override
    protected IOReader createReader(Path path) throws IOException {
        return new IOFileChannelReader(path);
    }

    @Override
    protected boolean requiresNativeDiskAccess() {
        return false;
    }
}

