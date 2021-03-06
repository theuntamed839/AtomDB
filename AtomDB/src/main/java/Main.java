import com.google.common.base.Stopwatch;
import db.DB;
import db.DBImpl;
import db.DBOptions;
import org.xerial.snappy.Snappy;

import java.time.Instant;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static util.BytesConverter.bytes;

public class Main {
//    public static void main1(String[] args) throws Exception {
//        int total = 10;
//        db.DBOptions opt = new db.DBOptions("ExampleDB");
//        Mem.MemtableManager memtableManager = new Mem.MemtableManager(opt);
//
//        for (int i = 0; i < total; i++) {
//            memtableManager.put(bytes(i + ""),
//                    bytes("value for " + i));
//        }
//
//        Scanner scan = new Scanner(System.in);
//        while (true) {
//            String command = scan.nextLine().trim();
//            if (command.equalsIgnoreCase("put")) {
//                String[] split = scan.nextLine().trim().split("\\s+");
//                memtableManager.put(bytes(split[0].trim()), bytes(split[1].trim()));
//            }else if (command.equalsIgnoreCase("delete")) {
//                memtableManager.delete(bytes(scan.nextLine().trim()));
//            } else {
//                break;
//            }
//        }
//
//        memtableManager.close();
//
//        Logs.FileChannelLogReader fileChannelLogReader = new Logs.FileChannelLogReader(opt);
//        Map<byte[], byte[]> map = new ConcurrentSkipListMap<>(Arrays::compare);
//        fileChannelLogReader.readWAL(map);
//
//        for (int i = 0; i < total; i++) {
//            if (Arrays.compare(memtableManager.get(bytes(i + "")),
//                    map.get(bytes(i + ""))) != 0)System.out.println(i + " not found");
//        }
//
//        System.out.println("map now");
//        System.out.println(map.entrySet().stream().map(each ->
//                Map.entry(new String(each.getKey()), new String(each.getValue())))
//                .toList());
//        System.out.println(map.keySet().stream().map(String::new).toList());
//        System.out.println(map.values().stream().map(String::new).toList());
//    }

    public static void main(String[] args) throws Exception {
        int total = 10_000_0;

        String value = Instant.now().toString().repeat(10);
        long a, b;
        a = System.nanoTime();
        DBOptions opt = new DBOptions("Thread"+Thread.currentThread());
        DB db = new DBImpl(opt);
        diskAccessByThreadsSpeedTest(db, total, value);
        b = System.nanoTime();
        System.out.println(Thread.currentThread() + " took "+ (b -a));

        System.out.println("end test");
    }

    public static void diskAccessByThreadsSpeedTest(DB db, int total, String value) throws Exception {
        for (int i = 0; i < total; i++) {
            db.put(bytes(i + ""),
                    bytes(i + value));
        }
    }


    public static void main1(String[] args) throws Exception {
        int total = 10_000;
        DBOptions opt = new DBOptions("ExampleDB");
        DB db = new DBImpl(opt);
//        String value = "the big value".repeat(40);
        String value = Instant.now().toString().repeat(10);
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

        long a,b;
        a = System.currentTimeMillis();
        //uncompressed
        for (int i = 0; i < total; i++) {
            db.put(bytes(i + ""),
                    bytes(i + value));
        }
        System.out.println("my stuff");
        db.put(bytes(0 + ""), bytes(0 + "value11"));
        db.put(bytes(10 + ""), bytes(10 + "value11"));
        db.put(bytes(100 + ""), bytes(100 + "value11"));
        db.put(bytes(1000 + ""), bytes(1000 + "value11"));
        db.put(bytes(5 + ""), bytes(5 + "value11"));
        db.put(bytes(50 + ""), bytes(50 + "value11"));
        db.put(bytes(500 + ""), bytes(500 + "value11"));

        db.delete(bytes(4 + ""));
        db.delete(bytes(32 + ""));
        db.delete(bytes(128 + ""));
        db.delete(bytes(512 + ""));
        db.delete(bytes(1024 + ""));
        db.delete(bytes(8 + ""));
        db.delete(bytes(16 + ""));
        db.delete(bytes(64 + ""));

        for (int i = total; i < total * 2; i++) {
            db.put(bytes(i + ""),
                    bytes(i + value));
        }
        // compressed value
//        for (int i = 0; i < total; i++) {
//            db.put(bytes(i + ""),
//                    Snappy.compress(bytes(i + value))
//                    );
//        }
        // compressed key & value
//        for (int i = 0; i < total; i++) {
//            db.put(Snappy.compress(bytes(i + "")),
//                    Snappy.compress(bytes(i + value))
//            );
//        }

        b = System.currentTimeMillis();

        System.out.println("done writing took="+(b-a) + " milli");

        System.out.println("User Input");
        while(true) {
            String input = scan.nextLine().trim();
            if (input.equalsIgnoreCase("quit")) {
                break;
            }
            byte[] bytes = db.get(bytes(input));
            if (bytes == null) System.out.println("found null");
            else System.out.println(new String(bytes));
        }

        System.out.println("reading test");
        a = System.currentTimeMillis();

        // uncompressed
        for (int i = 0; i < total; i++) {
            byte[] foundValue = db.get(bytes(i + ""));
            if (foundValue == null) System.out.println("value found null key=" + i);
            else if (!Arrays.equals(foundValue, bytes(i + value)))
                System.out.println("value found different");
        }

        // compressed value
//        for (int i = 0; i < total; i++) {
//            byte[] foundValue = db.get(bytes(i + ""));
//            if (foundValue == null) System.out.println("value found null key=" + i);
//            else if (!Arrays.equals(Snappy.uncompress(foundValue), bytes(i + value)))
//                System.out.println("value found different");
//        }

        // compressed key & value
//        for (int i = 0; i < total; i++) {
//            byte[] foundValue = db.get(Snappy.compress(bytes(i + "")));
//            if (foundValue == null) System.out.println("value found null key=" + i);
//            else if (!Arrays.equals(Snappy.uncompress(foundValue), bytes(i + value)))
//                System.out.println("value found different");
//        }

        b = System.currentTimeMillis();
        System.out.println("done reading, took="+(b-a) + " milli");
        db.close();db.destroy();
    }
}
/**
 * previous code
 * creating sst, size= 6092
 * sst.Header{versionId=123456789, entries=6092, sKey=0, lKey=999, binarySearchLocation=-9223372036854775808}
 * written
 * bs=4146492
 * done writing took=8805milli
 * quit
 * reading test
 * done reading, took=58728milli
 *
 * pointers writing optimization
 * creating sst, size= 6092
 * sst.Header{versionId=123456789, entries=6092, sKey=0, lKey=999, binarySearchLocation=-9223372036854775808}
 * written
 * bs=4146492
 * done writing took=4279milli
 * quit
 * reading test
 * done reading, took=64191milli
 *
 * reading pointers for binarysearch optimization
 * creating sst, size= 6092
 * sst.Header{versionId=123456789, entries=6092, sKey=0, lKey=999, binarySearchLocation=-9223372036854775808}
 * written
 * bs=4146492
 * done writing took=3806milli
 * quit
 * reading test
 * done reading, took=3732milli
 *
 * PureJava Leveldb
 * total = 10_000_0;
 * done writing took=7492milli
 * -1
 * found null
 * 0
 * 0hello_there asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas
 * quit
 * reading test
 * done reading, took=2563milli
 * //12:45 - 12/06/2022
 *
 * 10_000_0
 * uncompressed
 * done writing took=41622 milli
 * done reading, took=38778 milli
 *
 * 10_000_0
 * compressed value
 * done writing took=27713 milli
 * done reading, took=23053 milli
 *
 * compressed key & value
 * done writing took=27392 milli
 * done reading, took=30154 milli
 *
 * LEVELDB
 * 10_000_0
 * reading 3899
 * writing 3899
 */
