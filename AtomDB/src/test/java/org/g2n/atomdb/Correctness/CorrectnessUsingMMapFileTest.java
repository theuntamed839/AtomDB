package org.g2n.atomdb.Correctness;

import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;

public class CorrectnessUsingMMapFileTest extends CorrectnessTest{
    DbOptions opt;
    private Path dbPath;

    @Override
    protected DBImpl createDB() throws Exception {
        opt = new DbOptions();
        dbPath = Files.createTempDirectory("CorrectnessUsingDiskTest_" + Instant.now().toEpochMilli());
        return new DBImpl(dbPath, opt);
    }

    @Override
    protected void destroy() throws IOException {
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}