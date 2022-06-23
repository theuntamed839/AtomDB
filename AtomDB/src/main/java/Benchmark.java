import db.DBComparator;
import db.DBImpl;
import db.DBOptions;

import java.io.IOException;

import static util.BytesConverter.bytes;

public class Benchmark {
    DBOptions opt ;
    DBImpl db;
    String VALUE ;
    int TOTAL;

    public void init() throws Exception {
        opt = new DBOptions(this.getClass().getName() + "DB");
        db = new DBImpl(opt);
        VALUE = "value".repeat(50);
        TOTAL = 10_000_0;
    }

    public void closingSession() throws IOException {
        db.close();db.destroy();
    }

    public void bechmark() throws Exception {
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
        }
        b = System.nanoTime();

        readingTime = b - a;
        System.out.println("writing time=" + writingTime + " , reading time=" + readingTime);
    }

    public static void main(String[] args) throws Exception {
        Benchmark benchmark = new Benchmark();
        benchmark.init();
        benchmark.bechmark();
        benchmark.closingSession();
    }

}
