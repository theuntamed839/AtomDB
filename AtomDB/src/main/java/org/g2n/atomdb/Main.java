package org.g2n.atomdb;

import org.g2n.atomdb.db.DB;
import org.g2n.atomdb.db.DBImpl;
import org.g2n.atomdb.db.DbOptions;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Scanner;

import static org.g2n.atomdb.util.BytesConverter.bytes;
// todo, think/idea what if we store all the keys in memory, key -> fileToWrite
public class Main {
//    public static void main1(String[] args) throws Exception {
//        int total = 10;
//        org.g2n.atomdb.db.DbOptions opt = new org.g2n.atomdb.db.DbOptions("ExampleDB");
//        org.g2n.atomdb.Mem.SkipListMemtable memtableManager = new org.g2n.atomdb.Mem.SkipListMemtable(opt);
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
//        org.g2n.atomdb.Logs.FileChannelLogReader fileChannelLogReader = new org.g2n.atomdb.Logs.FileChannelLogReader(opt);
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
        DbOptions opt = new DbOptions();
        DB db = new DBImpl(new File("Thread"+Thread.currentThread()), opt);
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
        DbOptions opt = new DbOptions();
        DB db = new DBImpl(new File("ExampleDB"), opt);
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
//            org.g2n.atomdb.db.put(bytes(i + ""),
//                    Snappy.compress(bytes(i + value))
//                    );
//        }
        // compressed key & value
//        for (int i = 0; i < total; i++) {
//            org.g2n.atomdb.db.put(Snappy.compress(bytes(i + "")),
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
//            byte[] foundValue = org.g2n.atomdb.db.get(bytes(i + ""));
//            if (foundValue == null) System.out.println("value found null key=" + i);
//            else if (!Arrays.equals(Snappy.uncompress(foundValue), bytes(i + value)))
//                System.out.println("value found different");
//        }

        // compressed key & value
//        for (int i = 0; i < total; i++) {
//            byte[] foundValue = org.g2n.atomdb.db.get(Snappy.compress(bytes(i + "")));
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
 * creating org.g2n.atomdb.sst, size= 6092
 * org.g2n.atomdb.sst.Header{versionId=123456789, entries=6092, sKey=0, lKey=999, binarySearchLocation=-9223372036854775808}
 * written
 * bs=4146492
 * done writing took=8805milli
 * quit
 * reading test
 * done reading, took=58728milli
 *
 * pointers writing optimization
 * creating org.g2n.atomdb.sst, size= 6092
 * org.g2n.atomdb.sst.Header{versionId=123456789, entries=6092, sKey=0, lKey=999, binarySearchLocation=-9223372036854775808}
 * written
 * bs=4146492
 * done writing took=4279milli
 * quit
 * reading test
 * done reading, took=64191milli
 *
 * reading pointers for binarysearch optimization
 * creating org.g2n.atomdb.sst, size= 6092
 * org.g2n.atomdb.sst.Header{versionId=123456789, entries=6092, sKey=0, lKey=999, binarySearchLocation=-9223372036854775808}
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

// firefly
//Warm Up with 50k
//Writing... 50000
//Reading...
//writing time=1199177900 , reading time=1393644400
//Writing... 1000
//Reading...
//writing time=20145900 , reading time=35852900
//Writing... 10000
//Reading...
//writing time=256371600 , reading time=234508600
//Writing... 100000
//Reading...
//writing time=1272501300 , reading time=2226460200
//Writing... 1000000
//Reading...
//writing time=15237828200 , reading time=22578177500
// 15 and 22

//leveldb
//Warm Up with 50k
//Writing... 50000
//Reading...
//writing time=477759600 , reading time=152400700
//Writing... 1000
//Reading...
//writing time=9359800 , reading time=1390100
//Writing... 10000
//Reading...
//writing time=79710100 , reading time=14241500
//Writing... 100000
//Reading...
//writing time=780837900 , reading time=276792700
//Writing... 1000000
//Reading...
//writing time=10130438600 , reading time=2697346200

//atomDB
//Warm Up with 50k
//Writing... 50000
//Reading...
//writing time=961983000 , reading time=7546111700
//Writing... 1000
//Reading...
//writing time=4872700 , reading time=414700
//Writing... 10000
//Reading...
//writing time=44763700 , reading time=4071100
//Writing... 10000_0
//Reading...
//writing time=3879929800 , reading time=18500214900
//Writing... 10000_00
//Reading...
//writing time=95125721700 , reading time=204819288000
//95 sec and 3.4 minutes
//https://github.com/fusesource/leveldbjni
//https://www.reddit.com/r/developersIndia/comments/1aj1s2d/i_built_fireflydb_a_fast_keyvalue_storage_engine/
//https://github.com/godcrampy/fireflydb


// todo ideas
// instead of compressing key and value seperately, can directly compress key value while storing
