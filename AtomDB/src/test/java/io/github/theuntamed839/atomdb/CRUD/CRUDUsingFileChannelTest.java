package io.github.theuntamed839.atomdb.CRUD;

import io.github.theuntamed839.atomdb.db.AtomDB;
import io.github.theuntamed839.atomdb.db.DbOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    @Test
    public void testOpeningDbOnSameDirectoryRaisesError() throws Exception {
        var dbPath = getDBPath();
        try (var _ = new AtomDB(dbPath, new DbOptions())) {
            Assertions.assertThrows(IllegalStateException.class, () -> {
                try (var _ = new AtomDB(dbPath, new DbOptions())) {
                }
            });
        }
    }
}
