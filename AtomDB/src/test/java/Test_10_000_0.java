
import org.g2n.atomdb.db.DBComparator;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.g2n.atomdb.util.BytesConverter.bytes;

import java.io.File;
import java.nio.file.Path;

public class Test_10_000_0{
    DbOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;

    @BeforeEach
    public void init() throws Exception {
        opt = new DbOptions();
        db = new DBImpl(Path.of(this.getClass().getName() + "DB"), opt);
        VALUE = "value".repeat(50);
        TOTAL = 10_000_00;
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();db.destroy();
    }

    @Test
    public void test() throws Exception {
        long a , b, readingTime, writingTime;
        System.out.println("Writing... 10_000_0");
        a = System.nanoTime();
        for (int i = 0; i < TOTAL; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }
        b = System.nanoTime();

        writingTime = b - a;

        System.out.println("Reading");
        a = System.nanoTime();
        for (int i = 0; i < TOTAL; i++) {
            byte[] found = db.get(bytes(i + ""));

            Assertions.assertEquals(
                    DBComparator.byteArrayComparator
                            .compare(found, bytes(i + "_" + VALUE)),
                    0,
                    "found="+(new String(found))+" , checking with="+(new String(i + "_" + VALUE))
            );
        }
        b = System.nanoTime();
        
        readingTime = b - a;
        System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
    }
}
