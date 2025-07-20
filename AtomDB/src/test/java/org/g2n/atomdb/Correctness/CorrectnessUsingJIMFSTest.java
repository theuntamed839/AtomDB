package org.g2n.atomdb.Correctness;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.time.Instant;

public class CorrectnessUsingJIMFSTest extends CorrectnessTest{
    DbOptions opt;
    private FileSystem jimfs;

    @Override
    protected DBImpl createDB() throws Exception {
        opt = new DbOptions();
        opt.disallowUseOfMMap();
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        var dbPath = Files.createTempDirectory(jimfs.getPath("/"), "CorrectnessUsingJIMFSTest_" + Instant.now().toEpochMilli());
        return new DBImpl(dbPath, opt);
    }

    @Override
    protected void destroy(DB db) throws Exception {
        db.close();
        db.destroy();
        jimfs.close();
    }
}
