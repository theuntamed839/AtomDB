import db.DBImpl;
import db.DBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static util.BytesConverter.bytes;

import java.io.IOException;
import java.util.*;

public class UpdationDeletionTest {
    DBOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;
    int NUM_OP;
    String UPDATED_VALUE;

    enum Operation {
        DELETE,UPDATE
    }

    @BeforeEach
    public void init() throws Exception {
        opt = new DBOptions(this.getClass().getName() + "DB");
        db = new DBImpl(opt);
        VALUE = "value".repeat(50);
        TOTAL = 10_000_0;
        NUM_OP = 10_000;
        UPDATED_VALUE = "updatedValue".repeat(30);
    }

    @AfterEach
    public void closingSession() throws IOException {
        db.close();
        db.destroy();
    }
    
    @Test
    public void test() throws Exception {
        System.out.println("Writing "+TOTAL+" records");

        for (int i = 0; i < TOTAL; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        System.out.println("Writing more data so that sst's are flushed");
        var trashValue = "eulav".repeat(50);
        for (int i = TOTAL; i < TOTAL + 10_000; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + trashValue));
        }

        System.out.println("Random key to random operation(delete and update)");
        Map<Integer, Operation> map = getRandomOPWithKeys(NUM_OP, TOTAL);

        System.out.println("writing random keys");
        for (Integer key : map.keySet()) {
            if (map.get(key) == Operation.DELETE) {
                System.out.println("deleting " + key);
                db.delete(bytes(key + ""));
            }
            else {
                System.out.println("updating");
                db.put(
                        bytes(key + ""),
                        bytes(key + "_" + UPDATED_VALUE)
                        );
            }
        }
        System.out.println("Writing more data so that sst's are flushed");

        for (int i = TOTAL + 10_000; i < TOTAL + 20_000; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + trashValue));
        }

        System.out.println("Testing...");
        boolean isFailed = false;
        for (Integer key : map.keySet()) {
            if (map.get(key) == Operation.DELETE) {
                byte[] found = db.get(bytes(key + ""));
                if (found != null) {
                    isFailed = true;
                    System.out.println("key("+key+") not deleted, found value="+(new String(found)));
                }
            }else {
                byte[] found = db.get(bytes(key + ""));
                if (Arrays.compare(found, bytes(key + "_" + UPDATED_VALUE))
                        != 0) {
                    isFailed = true;
                    if (found != null)
                        System.out.println("key("+key+") not updated, value found="+ (new String(found)));
                    else
                        System.out.println("key("+key+") not updated, value found null");
                }
            }
        }
        Assertions.assertFalse(isFailed);
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
