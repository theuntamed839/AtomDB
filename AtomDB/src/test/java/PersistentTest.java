

import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.g2n.atomdb.util.BytesConverter.bytes;

public class PersistentTest {
    DbOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;
    int NUM_OP;
    String UPDATED_VALUE;

    record StartToEndLimits(int start, int end, Map<Integer, Operation> updateDeleteMap){}

    enum Operation {
        DELETE,UPDATE
    }

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        db = new DBImpl(Path.of(this.getClass().getName() + "DB"), opt);
        VALUE = "value".repeat(50);
        TOTAL = 10_000_0;
        NUM_OP = 10_000;
        UPDATED_VALUE = "updatedValue".repeat(30);
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();
        db.destroy();
    }

    @Test
    public void test() throws Exception {
        System.out.println("loading data, along with deletion and updates");
        StartToEndLimits startToEndLimits = putData();

        System.out.println("closing org.g2n.atomdb.db");
        // closing org.g2n.atomdb.db
        db.close();
        System.gc();

        System.out.println("reInit org.g2n.atomdb.db");
        // restarting
        init();

        System.out.println("persistent test ..");

        for (int key = startToEndLimits.start; key < startToEndLimits.end; key++) {
            byte[] found = db.get(bytes(key + ""));

            if (startToEndLimits.updateDeleteMap.containsKey(key)) {
                switch (startToEndLimits.updateDeleteMap.get(key)) {
                    case UPDATE -> Assertions.assertArrayEquals(
                            found, bytes(key + "_" + UPDATED_VALUE));
                    case DELETE -> Assertions.assertNull(found);
                }
            } else {
                Assertions.assertArrayEquals(
                        found, bytes(key + "_" + VALUE));
            }
        }
    }
    public StartToEndLimits putData() throws Exception {
        System.out.println("Writing " + TOTAL + " records");

        for (int i = 0; i < TOTAL; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        System.out.println("Writing more data so that org.g2n.atomdb.sst's are flushed");
        for (int i = TOTAL; i < TOTAL + 10_000; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        System.out.println("Random key to random operation(delete and update)");
        Map<Integer, Operation> map = getRandomOPWithKeys(NUM_OP, TOTAL);

        System.out.println("writing random keys");
        for (Integer key : map.keySet()) {
            if (map.get(key) == Operation.DELETE) {
                System.out.println("deleting " + key);
                db.delete(bytes(key + ""));
            } else {
                System.out.println("updating");
                db.put(
                        bytes(key + ""),
                        bytes(key + "_" + UPDATED_VALUE)
                );
            }
        }
        System.out.println("Writing more data so that org.g2n.atomdb.sst's are flushed");

        for (int i = TOTAL + 10_000; i < TOTAL + 20_000; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        System.out.println("Testing...");
        boolean isFailed = false;
        for (Integer key : map.keySet()) {
            if (map.get(key) == Operation.DELETE) {
                byte[] found = db.get(bytes(key + ""));
                if (found != null) {
                    isFailed = true;
                    System.out.println("key(" + key + ") not deleted, found value=" + (new String(found)));
                }
            } else {
                byte[] found = db.get(bytes(key + ""));
                if (Arrays.compare(found, bytes(key + "_" + UPDATED_VALUE))
                        != 0) {
                    isFailed = true;
                    if (found != null)
                        System.out.println("key(" + key + ") not updated, value found=" + (new String(found)));
                    else
                        System.out.println("key(" + key + ") not updated, value found null");
                }
            }
        }
        Assertions.assertFalse(isFailed);
        return new StartToEndLimits(0, TOTAL + 20_000, map);
    }

    private static Map<Integer, Operation> getRandomOPWithKeys(int n, int bound) {
        Random rand = new Random();
        Operation[] operations = Operation.values();
        Map<Integer, Operation> map = new HashMap<>(n);
        for (; map.size() < n;) {
            map.put(
                    rand.nextInt(bound),
                    operations[rand.nextInt(2)]
            );
        }
        return map;
    }
}
