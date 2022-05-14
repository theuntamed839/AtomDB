import static util.BytesConverter.bytes;

import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentSkipListMap;

public class Main {
    public static void main(String[] args) throws Exception {
        int total = 10;
        DBOptions opt = new DBOptions("ExampleDB");
        MemtableManager memtableManager = new MemtableManager(opt);

        for (int i = 0; i < total; i++) {
            memtableManager.put(bytes(i + ""),
                    bytes("value for " + i));
        }

        Scanner scan = new Scanner(System.in);
        while (true) {
            String command = scan.nextLine().trim();
            if (command.equalsIgnoreCase("put")) {
                String[] split = scan.nextLine().trim().split("\\s+");
                memtableManager.put(bytes(split[0].trim()), bytes(split[1].trim()));
            }else if (command.equalsIgnoreCase("delete")) {
                memtableManager.delete(bytes(scan.nextLine().trim()));
            } else {
                break;
            }
        }

        memtableManager.close();

        FileChannelLogReader fileChannelLogReader = new FileChannelLogReader(opt);
        Map<byte[], byte[]> map = new ConcurrentSkipListMap<>(Arrays::compare);
        fileChannelLogReader.readWAL(map);

        for (int i = 0; i < total; i++) {
            if (Arrays.compare(memtableManager.get(bytes(i + "")),
                    map.get(bytes(i + ""))) != 0)System.out.println(i + " not found");
        }

        System.out.println("map now");
        System.out.println(map.entrySet().stream().map(each ->
                Map.entry(new String(each.getKey()), new String(each.getValue())))
                .toList());
        System.out.println(map.keySet().stream().map(String::new).toList());
        System.out.println(map.values().stream().map(String::new).toList());
    }
}
