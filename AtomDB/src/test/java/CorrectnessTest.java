import db.DBImpl;
import db.DbOptions;
import org.junit.jupiter.api.*;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static util.BytesConverter.bytes;

/**
 *  1. write records and read sequentially (forward and backward)
 *  2. write records and read randomly (all)
 *
 */
public class CorrectnessTest {
    public static final int TOTAL = 500000;
    public static final String VALUE = "value".repeat(50);
    DbOptions opt ;
    DBImpl db;

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        db = new DBImpl(new File(this.getClass().getName() +"_"+ Instant.now().getEpochSecond()+ "_DB"), opt);
        for (int i = 0; i < TOTAL; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();db.destroy();
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

