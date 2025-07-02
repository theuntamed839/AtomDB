
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.g2n.atomdb.util.BytesConverter.bytes;

/**
 * 1. write records and read sequentially (forward and backward)
 * 2. write records and read randomly (all)
 */
public class CorrectnessTest {
//        public static final int TOTAL = 500000;
    public static final int TOTAL = 10_000_00;
    public static final String VALUE = "value".repeat(500);
    DbOptions opt;
    DBImpl db;
    private FileSystem jimfs;

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        opt.disallowUseOfMMap();
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        var dbPath = Files.createTempDirectory(jimfs.getPath("/"), "benchmarkWithRandomKVBytes_" + getSaltString());
        db = new DBImpl(dbPath, opt);
        for (int i = 0; i < TOTAL; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();
        db.destroy();
        jimfs.close();
    }

    static String getSaltString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }

    @Test
    public void sequentialReadForward() throws Exception {
        for (int i = 0; i < TOTAL; i++) {
            if (i % 10000 == 0) {
                System.out.println(i);
            }
            Assertions.assertArrayEquals(db.get(bytes(i + "")), bytes(i + "_" + VALUE));
        }
        System.out.println("done");
    }

    @Test
    public void sequentialReadBackward() throws Exception {
        for (int i = TOTAL - 1; i >= 0; i--) {
            if (i % 10000 == 0) {
                System.out.println(i);
            }
            Assertions.assertArrayEquals(db.get(bytes(i + "")), bytes(i + "_" + VALUE));
        }
        System.out.println("done");
    }

    @Test
    public void randomRead() throws Exception {
        List<Integer> integers = new ArrayList<>(IntStream.range(0, TOTAL).boxed().toList());
        Collections.shuffle(integers);
        Collections.shuffle(integers);
        for (int i : integers) {
            if (i % 10000 == 0) {
                System.out.println(i);
            }
            Assertions.assertArrayEquals(db.get(bytes(i + "")), bytes(i + "_" + VALUE));
        }
        System.out.println("done");
    }
}

