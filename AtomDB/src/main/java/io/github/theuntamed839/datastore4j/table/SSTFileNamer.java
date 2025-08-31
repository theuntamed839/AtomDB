package io.github.theuntamed839.datastore4j.table;

import io.github.theuntamed839.datastore4j.level.Level;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SSTFileNamer {
    private final Path directory;
    private final AtomicLong counter;

    public SSTFileNamer(Path directory) {
        this.directory = directory;
        this.counter = new AtomicLong(findLastSequenceNumber(directory));
    }

    private long findLastSequenceNumber(Path directory) {
        try (var stream = Files.list(directory)) {
            return stream
                    .parallel()
                    .map(Path::getFileName)
                    .filter(this::isValidSST)
                    .map(Path::toString)
                    .map(n -> n.replace("SST_", "").replace(".sst", ""))
                    .mapToLong(n -> Long.parseLong(n.split("_")[1]))
                    .max().orElse(0L);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SSTFileNameMeta nextSst(Level level) {
        long seq = counter.incrementAndGet();
        return new SSTFileNameMeta(directory.resolve("SST_%d_%d.sst".formatted(level.value(), seq)), level, seq);
    }

    public SSTFileNameMeta parse(Path filePath) {
        String name = filePath.getFileName().toString();
        String[] parts = name.split("[_.]");
        var level = Level.of(Integer.parseInt(parts[1]));
        long seq = Long.parseLong(parts[2]);
        return new SSTFileNameMeta(filePath, level, seq);
    }

    public Set<SSTFileNameMeta> getValidSSTFiles() {
        try (var stream = Files.list(directory)) {
            return stream
                    .parallel()
                    .filter(this::isValidSST)
                    .map(this::parse)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isValidSST(Path path) {
        return path.getFileName().toString().matches("SST_\\d+_\\d+\\.sst");
    }
}