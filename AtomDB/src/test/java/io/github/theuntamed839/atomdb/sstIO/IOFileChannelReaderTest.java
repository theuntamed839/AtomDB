package io.github.theuntamed839.atomdb.sstIO;

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

