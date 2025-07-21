package org.g2n.atomdb.correctness;

import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.io.File;
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
    protected void destroy(DB db) throws Exception {
        db.close();
        db.destroy();
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}