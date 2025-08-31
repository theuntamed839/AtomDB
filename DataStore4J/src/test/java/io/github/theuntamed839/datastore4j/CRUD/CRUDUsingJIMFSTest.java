package io.github.theuntamed839.datastore4j.CRUD;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class CRUDUsingJIMFSTest extends CRUDTest {
    private final FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix());
    
    @Override
    protected Path getDBPath() throws IOException {
        return Files.createTempDirectory(jimfs.getPath("/"), "CorrectnessUsingJIMFSTest_" + Instant.now().toEpochMilli());
    }

    @Override
    protected boolean shouldDisableMMap() {
        return true;
    }

    @Override
    protected void close() throws IOException {
        if (jimfs != null) {
            jimfs.close();
        }
    }
}
