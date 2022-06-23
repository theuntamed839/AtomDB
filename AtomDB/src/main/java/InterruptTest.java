import db.DB;
import db.DBImpl;
import db.DBOptions;

import java.time.Instant;
import java.util.Scanner;

import static util.BytesConverter.bytes;

public class InterruptTest {

    public static void main(String[] args) throws Exception {
        int total = 10_000_0;

        String value = Instant.now().toString().repeat(10);
        DBOptions opt = new DBOptions("InterruptTest");
        DB db = new DBImpl(opt);
        System.out.println("User Input");
        Scanner scan = new Scanner(System.in);
        while(true) {
            String input = scan.nextLine().trim();
            if (input.equalsIgnoreCase("quit")) {
                break;
            }
            byte[] bytes = db.get(bytes(input));
            if (bytes == null) System.out.println("found null");
            else System.out.println(new String(bytes));
        }

//        for (int i = 0; i < total; i++) {
//            System.out.println("writing " + i);
//            db.put(bytes(i + ""), bytes(value));
//        }
    }
//    2276
}
