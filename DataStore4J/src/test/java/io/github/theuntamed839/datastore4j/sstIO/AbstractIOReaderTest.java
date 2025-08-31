package io.github.theuntamed839.datastore4j.sstIO;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.theuntamed839.datastore4j.util.BytesConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
abstract class AbstractIOReaderTest {
    protected FileSystem jimfs;
    protected Path testFilePath;
    protected IOReader reader;

    protected static final byte[] TEST_DATA_BYTES = {
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0A, 0x0B, 0x0C,
            0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
            0x17, 0x18, 0x19, 0x20,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x03, //72623859790382851
    };

    protected abstract IOReader createReader(Path path) throws IOException;
    protected abstract boolean requiresNativeDiskAccess();

    @BeforeEach
    void setUpAbstract() throws IOException {
        if (requiresNativeDiskAccess()) {
            testFilePath = File.createTempFile("testfile", ".bin").toPath();
        } else {
            jimfs = Jimfs.newFileSystem(Configuration.unix());
            testFilePath = jimfs.getPath("testfile.bin");
        }

        Files.write(testFilePath, TEST_DATA_BYTES);
        reader = createReader(testFilePath);
    }

    @AfterEach
    void tearDownAbstract() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (jimfs != null) {
            jimfs.close();
        }
    }

    @Test
    void testReadSingleByte() {
        try {
            assertEquals(0x01, reader.read());
            reader.position(3);
            assertEquals(0x04, reader.read());
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadSingleByteEOF() {
        try {
            reader.position(TEST_DATA_BYTES.length);

            assertEquals(-1, reader.read());
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadArrOfBytes() {
        try {
            var input = new byte[8];
            var expected = new byte[8];
            System.arraycopy(TEST_DATA_BYTES, 0, expected, 0, input.length);

            int readByteCount = reader.read(input);

            assertEquals(8, readByteCount);
            assertArrayEquals(expected, input);
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadArrOfBytesMiddleOfFile() {
        try {
            reader.position(TEST_DATA_BYTES.length - 3);
            var input = new byte[8];
            var expected = new byte[8];
            System.arraycopy(TEST_DATA_BYTES, TEST_DATA_BYTES.length - 3, expected, 0, 3);

            int readByteCount = reader.read(input);

            assertEquals(3, readByteCount);
            assertArrayEquals(expected, input);
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadArrOfBytesEOF() {
        try {
            reader.position(TEST_DATA_BYTES.length);
            var input = new byte[8];

            int readByteCount = reader.read(input);

            assertEquals(-1, readByteCount);
            assertArrayEquals(new byte[8], input);
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadArrOfBytesWithRange() {
        try {
            var input = new byte[20];
            var expected = new byte[20];
            System.arraycopy(TEST_DATA_BYTES, 0, expected, 5, 10);

            int readByteCount = reader.read(input, 5, 10);

            assertEquals(10, readByteCount);
            assertArrayEquals(expected, input);
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadArrOfBytesWithRangeMiddleOfFile() {
        try {
            reader.position(TEST_DATA_BYTES.length - 3);
            var input = new byte[8];
            var expected = new byte[8];
            System.arraycopy(TEST_DATA_BYTES, TEST_DATA_BYTES.length - 3, expected, 2, 3);

            int readByteCount = reader.read(input, 2, 6);

            assertEquals(3, readByteCount);
            assertArrayEquals(expected, input);
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testReadArrOfBytesWithRangeBoundCheck() {
        var input = new byte[8];
        assertThrows(IndexOutOfBoundsException.class, () -> {
            reader.read(input, -2, 10);
        });
        assertThrows(IndexOutOfBoundsException.class, () -> {
            reader.read(input, 2, -10);
        });
        assertThrows(IndexOutOfBoundsException.class, () -> {
            reader.read(input, 10, 10);
        });
        assertThrows(IndexOutOfBoundsException.class, () -> {
            reader.read(input, 2, 100);
        });
    }

    @Test
    void testReadArrOfBytesWithRangeEOF() {
        try {
            reader.position(TEST_DATA_BYTES.length);
            var bytes = new byte[8];

            int readByteCount = reader.read(bytes, 2, 6);

            assertEquals(-1, readByteCount);
            assertArrayEquals(new byte[8], bytes);
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void testGetLong() {
        try {
            long expected = BytesConverter.bytesToLong(Arrays.copyOf(TEST_DATA_BYTES, Long.BYTES));
            assertEquals(expected, reader.getLong());

            reader.position(TEST_DATA_BYTES.length - Long.BYTES);

            expected = BytesConverter.bytesToLong(Arrays.copyOfRange(TEST_DATA_BYTES, TEST_DATA_BYTES.length - Long.BYTES,TEST_DATA_BYTES.length + Long.BYTES));
            assertEquals(expected, reader.getLong());
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void position() {
        try {
            reader.position(0);
            assertEquals(0, reader.position());

            reader.position(TEST_DATA_BYTES.length - 1);
            assertEquals(TEST_DATA_BYTES.length - 1, reader.position());

            reader.position(TEST_DATA_BYTES.length);
            assertEquals(TEST_DATA_BYTES.length, reader.position());
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }

    @Test
    void hasRemaining() {
        try {
            assertTrue(reader.hasRemaining());
            reader.position(TEST_DATA_BYTES.length / 2);
            assertTrue(reader.hasRemaining());
            reader.position(TEST_DATA_BYTES.length);
            assertFalse(reader.hasRemaining());
        } catch (IOException e) {
            fail("IOException occurred: " + e.getMessage());
        }
    }
}