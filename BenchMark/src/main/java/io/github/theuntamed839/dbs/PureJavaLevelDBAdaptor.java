package io.github.theuntamed839.dbs;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class PureJavaLevelDBAdaptor implements BenchmarkDBAdapter {

    private final DB db;
    private final Path dbPath;

    public PureJavaLevelDBAdaptor() throws IOException {
        dbPath = Files.createTempDirectory("LEVELDB_NATIVE_" + LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        Options options = new Options();
        options.createIfMissing(true);
        db = factory.open(dbPath.toFile(), options);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        db.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return db.get(key);
    }

    @Override
    public void closeAndDestroy() throws IOException {
        db.close();
        try (Stream<Path> stream = Files.walk(dbPath)) {
            stream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}