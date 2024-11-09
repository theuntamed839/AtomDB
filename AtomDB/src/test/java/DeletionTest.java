import Table.Table;
import db.DBImpl;
import db.DbOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sst.ValueUnit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import static util.BytesConverter.*;

public class DeletionTest {
    DbOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;
    int N_TO_DELETE;

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        db = new DBImpl(new File(this.getClass().getName() + "DB"), opt);
        VALUE = "value".repeat(50);
        TOTAL = 10000;
        N_TO_DELETE = 1;
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
        List<Integer> toBeDeleted = getRandomNumberToDelete(N_TO_DELETE, TOTAL);
        System.out.println(toBeDeleted);

        System.out.println("deleting random");
        for (Integer integer : toBeDeleted) {
            db.delete(bytes(integer + ""));
        }

//        while(exitsValueInSSTFiles(toBeDeleted, db.getTable())) {
//            System.out.println("sending random bullshit");
//            for (int i = TOTAL; i < TOTAL + TOTAL; i++) {
//                db.put(bytes(i + ""), bytes(i + "_" + VALUE));
//            }
//        }
    }
    private static boolean exitsValueInSSTFiles(List<Integer> toBeDeleted, Table table) throws Exception {
        boolean result = false;
        for (int i = 0; i < 7; i++) {
            List<String> levelList = null;//table.getLevelFileList(Level.fromID(i));
            System.out.println("level=" + i);
            for (String file : levelList) {
                for (Integer key : toBeDeleted) {
                    ValueUnit valueUnit = BinarySearch.file(file, bytes(key + ""));

                    if (valueUnit == null) continue;

                    if (valueUnit.getIsDelete() == ValueUnit.DELETE) {
                        System.out.println("key="+key+" deleted for file="+file+" level="+i);
                        if (valueUnit.getValue() != null) System.out.println("what the fuck, value???");
                    } else {
                        result= true;
                        System.out.println("key="+key+" not deleted for file="+file+" level="+i);
                        if (valueUnit.getValue() == null) System.out.println("what the fuck, value!!!!");
                    }
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
