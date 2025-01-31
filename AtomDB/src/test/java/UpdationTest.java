package java;

import org.g2n.atomdb.Table.Table;
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.*;
import org.g2n.atomdb.sst.ValueUnit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import static org.g2n.atomdb.util.BytesConverter.bytes;

public class UpdationTest {
    DbOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;
    int N_TO_UPDATE ;

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        db = new DBImpl(new File(this.getClass().getName() + "DB"), opt);
        VALUE = "value".repeat(50);
        TOTAL = 10000;
        N_TO_UPDATE = 1;
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();db.destroy();
    }

    @Test
    public void test() throws Exception {

        Scanner scan = new Scanner(System.in);

        System.out.println("writing data");
        for (int i = 0; i < TOTAL * 2; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        System.out.println("getting random");
        List<Integer> toBeUpdated = getRandomNumberToDelete(N_TO_UPDATE, TOTAL);
        System.out.println(toBeUpdated);

        System.out.println("updating random");
        for (Integer integer : toBeUpdated) {
            db.put(bytes(integer + ""), bytes("updated value boss 69"));
        }


//        while(hasOldValueInSSTFiles(toBeUpdated, org.g2n.atomdb.db.getTable(), bytes("updated value boss 69"))) {
//            System.out.println("sending random bullshit");
//            for (int i = TOTAL; i < TOTAL + TOTAL; i++) {
//                org.g2n.atomdb.db.put(bytes(i + ""), bytes(i + "_" + VALUE));
//            }
//        }
    }

    private static boolean hasOldValueInSSTFiles(List<Integer> tobeUpdated, Table table, byte[] newValue) throws Exception {
        boolean result = false;
        for (int i = 0; i < 7; i++) {
            List<String> levelList = null;//table.getSSTInfoSet(org.g2n.atomdb.Level.fromID(i));
            System.out.println("level=" + i);
            for (String file : levelList) {
                for (Integer key : tobeUpdated) {
                    ValueUnit valueUnit = BinarySearch.file(file, bytes(key + ""));

                    if (valueUnit == null) {
                        continue;
                    }

                    if (DBComparator.byteArrayComparator.compare(
                            valueUnit.getValue(), newValue) == 0) {
                        System.out.println("key="+key+" updated for fileToWrite="+file+" level="+i);
                    } else {
                        result= true;
                        System.out.println("key="+key+" not updated for fileToWrite="+file+" level="+i);
                    }
                    if (valueUnit.getIsDelete() == ValueUnit.DELETE) System.out.println("deleted ???");
                }
            }
        }
        return result;
    }

    private static List<Integer> getRandomNumberToDelete(int n, int bound) {
        Random rand = new Random();
        List<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(rand.nextInt(bound));
        }
        return list;
    }
}
