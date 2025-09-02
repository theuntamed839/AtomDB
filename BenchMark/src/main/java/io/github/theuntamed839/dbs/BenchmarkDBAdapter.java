package io.github.theuntamed839.dbs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public interface BenchmarkDBAdapter {
    void put(byte[] key, byte[] value) throws Exception;
    byte[] get(byte[] key) throws Exception;
    void closeAndDestroy() throws Exception;

    default long getDirectorySize(Path directory) throws IOException {
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return 0L;
        }

        try (var stream = Files.walk(directory)) {
            return stream
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            System.err.println("Could not get size of " + p + ": " + e.getMessage());
                            return 0L;
                        }
                    })
                    .sum() / (1024L * 1024L); // Convert bytes to megabytes
        }
    }

    default void deleteDirectory(Path dbPath) {
        try (var stream = Files.walk(dbPath)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException e) {
            System.out.println("Failed to delete directory: " + e.getMessage());
        }
    }
}
