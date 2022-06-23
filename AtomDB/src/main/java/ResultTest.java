import db.DB;
import db.DBImpl;
import db.DBOptions;

import java.time.Instant;
import java.util.Scanner;

import static util.BytesConverter.bytes;

public class ResultTest {
    public static void main(String[] args) throws Exception {
        int total = 10_000;
        DBOptions opt = new DBOptions("ResultTest");
        DB db = new DBImpl(opt);
        System.out.println("Starting DB...");
        String VALUE = Instant.now().toString().repeat(10);

        Scanner scan = new Scanner(System.in);
        while(true) {
            String input = scan.nextLine().trim();
            String[] pieces = input.split("\\s+");
            if (pieces[0].equalsIgnoreCase("quit")) {
                break;
            }
            if (pieces[0].equalsIgnoreCase("#")) {
                continue;
            }

            if (pieces[0].equalsIgnoreCase("randomData")) {
                for (int i = 0; i < total; i++) {
                    db.put(bytes(i + ""),
                            bytes(i + VALUE));
                }
            }

            switch (pieces[0].toUpperCase()) {
                case "PUT" -> db.put(bytes(pieces[1]), bytes(pieces[2]));
                case "GET" -> {
                    byte[] bytes = db.get(bytes(pieces[1]));
                    if (bytes == null) System.out.println("null");
                    else System.out.println(new String(bytes));
                }
                case "DELETE" -> db.delete(bytes(pieces[1]));
            }
        }
        db.close();
        db.destroy();
    }
}
/**

 # adding few key values
 PUT user-123 {name:John_friend,age:26,edu:Engineer}
 PUT user-234 {name:Kenny_S,age:33,edu:Doctor}

 # adding random data so that previous KVs are flushed to disk
 RandomData 10000

 # retrieving previous keys
 GET user-234

 */