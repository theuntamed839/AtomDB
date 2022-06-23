package Bech;

import db.DBComparator;
import db.DBImpl;
import db.DBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static util.BytesConverter.bytes;

public class Test_10_000_00 {
    DBOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;

    @BeforeEach
    public void init() throws Exception {
        opt = new DBOptions(this.getClass().getName() + "DB");
        db = new DBImpl(opt);
        VALUE = "value".repeat(50);
        TOTAL = 10_000_00;
    }

    @AfterEach
    public void closingSession() throws IOException {
        db.close();db.destroy();
    }

    @Test
    public void test() throws Exception {
        long a , b, readingTime, writingTime;
        System.out.println("Writing... 10_000_00");
        a = System.nanoTime();
        for (int i = 0; i < TOTAL; i++) {
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }
        b = System.nanoTime();

        writingTime = b - a;

        System.out.println("Reading");
        a = System.nanoTime();
        for (int i = 0; i < TOTAL; i++) {
            db.get(bytes(i + ""));
        }
        b = System.nanoTime();

        readingTime = b - a;
        System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
    }
}
