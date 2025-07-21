package org.g2n.atomdb.CRUD;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class CRUDUsingFileChannelTest extends CRUDTest {
    @Override
    protected Path getDBPath() throws IOException {
        return Files.createTempDirectory("CorrectnessUsingFileChannelTest_" + Instant.now().toEpochMilli());
    }

    @Override
    protected boolean shouldDisableMMap() {
        return true;
    }
}
