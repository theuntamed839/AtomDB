package io.github.theuntamed839.datastore4j.sstIO;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractIOWriterTest {
    protected FileSystem jimfs;
    protected Path testFilePath;
    protected IOWriter writer;

    protected static final byte[] TEST_DATA_BYTES = {
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0A, 0x0B, 0x0C,
            0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
            0x17, 0x18, 0x19, 0x20,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x03, //72623859790382851
    };

    protected abstract IOWriter createWriter(Path path, long fileSize) throws IOException;

    protected abstract boolean requiresNativeDiskAccess();

    @BeforeEach
    void setUpAbstract() throws IOException {
        if (requiresNativeDiskAccess()) {
            testFilePath = File.createTempFile("testfile", ".bin").toPath();
        } else {
            jimfs = Jimfs.newFileSystem(Configuration.unix());
            testFilePath = Files.createTempFile(jimfs.getPath("/"), "testfile", ".bin");
        }
    }

    @AfterEach
    void tearDownAbstract() throws Exception {
        if (jimfs != null) {
            jimfs.close();
        }
    }

    @Test
    void putTest() throws Exception {
        writer = createWriter(testFilePath, TEST_DATA_BYTES.length);
        var testDataWrap = ByteBuffer.wrap(TEST_DATA_BYTES);

        writer.put(testDataWrap);
        writer.close();
        var output = Files.readAllBytes(testFilePath);

        assertArrayEquals(TEST_DATA_BYTES, output);
    }

    @Test
    void fileNeedsToExistsTest() {
        assertThrows(NoSuchFileException.class, () -> createWriter(Path.of(UUID.randomUUID().toString()), 123));
    }

    @Test
    void exceptionRaisedWhenClosedMoreThenOnce() throws Exception {
        writer = createWriter(testFilePath, TEST_DATA_BYTES.length);
        writer.close();
        assertThrows(IllegalStateException.class, () -> writer.close());
    }

    @Test
    void negativeFileSizeThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> createWriter(testFilePath, -1));
    }

    @Test
    void exceededSizeRaiseException() throws Exception {
        writer = createWriter(testFilePath, TEST_DATA_BYTES.length);
        ByteBuffer wrap = ByteBuffer.allocate(TEST_DATA_BYTES.length * 2);
        wrap.put(TEST_DATA_BYTES);
        wrap.put(TEST_DATA_BYTES);
        wrap.flip();

        assertThrows(java.nio.BufferOverflowException.class, () -> writer.put(wrap));
        writer.close();
    }
}
