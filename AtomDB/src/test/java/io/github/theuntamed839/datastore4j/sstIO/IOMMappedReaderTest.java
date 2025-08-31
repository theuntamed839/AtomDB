package io.github.theuntamed839.datastore4j.sstIO;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;

@DisplayName("IOMMappedReader Specific Tests")
class IOMMappedReaderTest extends AbstractIOReaderTest {

    @Override
    protected IOReader createReader(Path path) throws IOException {
        return new IOMMappedReader(path);
    }

    @Override
    protected boolean requiresNativeDiskAccess() {
        return true;
    }

}
