import com.google.common.primitives.Longs;
import org.xerial.snappy.PureJavaCrc32C;
import util.BytesConverter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32C;

/*
* todo
*  log reader and log writer both have lot of commmon things
*  try to reduce the duplication of code
* */

public class FileChannelLogReader implements LogReader{
    private static String LOG = "LOG";
    private File logFile = null;
    private ByteBuffer byteBuffer;
    //todo
    // this can be removed and bytebuffer api can be used to get the capacity
    private int currentByteBufferLimit = 4 * SizeOf.MBinBytes;
    private static final int delimiterLength = System.lineSeparator().getBytes(StandardCharsets.UTF_8).length; //1
    private static final byte[] delimiter = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
    private static final int DateTimeLength = 30;

    private static final PureJavaCrc32C crc32cChecksum = new PureJavaCrc32C();

    public FileChannelLogReader(DBOptions dbOptions) throws Exception {
        var folder = new File(dbOptions.getDBfolder());
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (file.isDirectory()) {
                throw new Exception("found folder(named='"+file.getName()+"') in DB folder ");
            }
            if (file.getName().contains(LOG) ){
                if (logFile != null) {
                    throw new Exception("multiple log files");
                }
                logFile = file;
            }
        }
        if (logFile == null) throw new Exception("No log file found");
        byteBuffer = ByteBuffer.allocate(4 * SizeOf.MBinBytes);
    }


    private void readFile() throws IOException {
        RandomAccessFile reader = new RandomAccessFile(logFile, "r");
        while(true) {
            String s = reader.readLine();
            if (s == null) {
                break;
            }
            System.out.println(s);
        }
    }

    @Override
    public void readWAL(Map<byte[], byte[]> map) throws Exception {
        LogBlock current = null;
        try (RandomAccessFile reader =
                     new RandomAccessFile(logFile, "r")) {
            FileChannel channel = reader.getChannel();

            for (long i = 0; i < channel.size(); i += current.totalBytesRead()) {
                current = LogBlock.read(channel, byteBuffer);
                switch (current.getOperations()) {
                    case WRITE -> map.put(current.getKey(), current.getValue());
                    case DELETE -> {
                        if (map.containsKey(current.getKey())) {
                            if (Arrays.compare(map.get(current.getKey()), current.getValue()) == 0) {
                                map.remove(current.getKey());
                                System.out.println(map.entrySet().stream().map(each ->
                                                Map.entry(new String(each.getKey()), new String(each.getValue())))
                                        .toList());
                            } else {
                                throw new Exception("previous value mismatch for the key");
                            }
                        } else {
                            throw new Exception("key not found in map");
                        }
                        System.out.println("after deleting");
                        System.out.println(map.entrySet().stream().map(each ->
                                        Map.entry(new String(each.getKey()), new String(each.getValue())))
                                .toList());
                    }
                    case UPDATE -> {
                        if (map.put(current.getKey(), current.getValue()) == null) {
                            throw new Exception("updation before any key entered");
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(new String(current.getKey()) + " - " + new String(current.getValue()));
            System.out.println(map.entrySet().stream().map(each ->
                            Map.entry(new String(each.getKey()), new String(each.getValue())))
                    .toList());

            throw e;
        }
    }
}
