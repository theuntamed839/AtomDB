import SsTable.MemtableManager;
import SsTable.SstableManager;
import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {
//        String value = """
//                {
//                    name:"myName",
//                    college:"college name",
//                    address:"my address"
//                }
//                """;
//        var a = new TreeSet<byte[]>(Arrays::compare);
//        for (int i = 0; i < 100; i++) {
//            a.add((i +"").getBytes(StandardCharsets.UTF_8));
//        }
//        for (byte[] bytes : a) {
//            System.out.println(new String(bytes));
//        }

        String value = "hello_there asdkjsahdhjksa aslhhdkjas" +
                "asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas" +
                "asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas"+
        "asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas"+
        "asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas"+
        "asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas"+
        "asdkjsahdhjksa aslhhdkjasasdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas asdkjsahdhjksa aslhhdkjas";
        var sstable = new SstableManager("ExampleDB");
        var mem = new MemtableManager(sstable);
        int TOTAl = 10_000_0;

        long writestart = System.currentTimeMillis();

        for (int i = 0; i < TOTAl; i++) {
//            byte[] temp = Snappy.compress(value + "This" + i);
//            mem.put((i + "").getBytes(StandardCharsets.UTF_8), temp);
            mem.put(i + "", value + "This" + i);
        }

        System.out.println("done writing");
        Scanner s1 = new Scanner(System.in);
        while(true) {
            String input = s1.nextLine().trim();
            if (input.equalsIgnoreCase("quit")
                    ||input.equalsIgnoreCase("exit")) {
                break;
            }
            byte[] bytes = mem.get(input);
            if (bytes == null) {
                System.out.println("value found null");
            }else {
//                System.out.println(new String(Snappy.uncompress(bytes)));
                System.out.println(new String(bytes));
            }
        }

        List<String> logs = new ArrayList<>(TOTAl);
//        read time188
//        write time334
        System.out.println("reading");
        long writeEnd = System.currentTimeMillis();
        long readStart = System.currentTimeMillis();
        for (int i = 0; i < TOTAl; i++) {
            byte[] foundValue = mem.get(i + "");
            if (foundValue == null) {
                logs.add(i + " found Null");
            }else {
                logs.add(new String(foundValue) + " " + (i + " found"));
            }
        }
        long readEnd = System.currentTimeMillis();
        logs.forEach(System.out::println);
        System.out.println("read time" + (readEnd - readStart));
        System.out.println("write time" + (writeEnd - writestart));
        Scanner s = new Scanner(System.in);
        while(true) {
            String input = s.nextLine().trim();
            if (input.equals("quit")) {
                break;
            }
            System.out.println(new String(mem.get(input)));
        }

        //Display

//        var sstable = new SstableManager("test111111");
//        ConcurrentSkipListMap<String, Map.Entry<byte[], byte[]>> map = new ConcurrentSkipListMap<>();
//        char letter = 'z';
//        for (int i = 0; i < 20; i++) {
//            map.put(String.valueOf(letter), Map.entry(
//                    String.valueOf(letter--).getBytes(StandardCharsets.UTF_8),
//                    (i + "").getBytes(StandardCharsets.UTF_8)
//                    ));
//        }
//        String fileName = sstable.write(map);
//        sstable.read(fileName);

//        var sstable = new SstableManager("ExampleDB");
//        ConcurrentSkipListMap<String, Map.Entry<byte[], byte[]>> map = new ConcurrentSkipListMap<>();
//        Scanner s = new Scanner(System.in);
//        while(true) {
//            String input = s.nextLine().trim();
//            if (input.equals("quit")) {
//                break;
//            }
//            var key = input.split("\\s+")[0];
//            var value = input.split("\\s+")[1];
//
//            map.put(key, Map.entry(
//                    key.getBytes(StandardCharsets.UTF_8),
//                    value.getBytes(StandardCharsets.UTF_8)
//                    ));
//        }
//        String fileName = sstable.write(map);
//        sstable.read(fileName);
//
    }
}
