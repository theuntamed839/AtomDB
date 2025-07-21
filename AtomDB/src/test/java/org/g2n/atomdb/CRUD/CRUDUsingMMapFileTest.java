package org.g2n.atomdb.CRUD;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class CRUDUsingMMapFileTest extends CRUDTest {
    @Override
    protected Path getDBPath() throws IOException {
        return Files.createTempDirectory("CorrectnessUsingDiskTest_" + Instant.now().toEpochMilli());
    }

    @Override
    protected boolean shouldDisableMMap() {
        return false;
    }
}