package Memtable;

import Constants.DBConstants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class LogWrite {

    FileChannel channel;
    static String LOG = "LOG";
    String currentFileName;
    ByteBuffer byteBuffer;
    int currentByteBufferLimit = 4 * DBConstants.MB;
    static int delimiterLength = "\n".getBytes(StandardCharsets.UTF_8).length; //1
    static byte[] delimiter = "\n".getBytes(StandardCharsets.UTF_8);

    public LogWrite() throws FileNotFoundException {
        byteBuffer = ByteBuffer.allocate(currentByteBufferLimit);
        refresh();
    }

    public void newLog() throws Exception {
        closeFile();
        if (!new File(currentFileName).delete()) {
            throw new Exception("log file not deleted");
        }
        refresh();
    }

    private void refresh() throws FileNotFoundException {
        currentFileName = LOG + "-" + LocalDateTime.now();
        channel = new RandomAccessFile(currentFileName,
                "rw").getChannel();
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    public void put(byte[] key, byte[] value) throws Exception {
        logWrite(key, value);
    }

    private void logWrite(byte[] key, byte[] value) throws Exception {
        int length = key.length + value.length + delimiterLength * 2;
        if (length > currentByteBufferLimit) {
            setByteBufferLimit(length);
        }
        byteBuffer.clear();
        byteBuffer.put(key);byteBuffer.put(delimiter);
        byteBuffer.put(value);byteBuffer.put(delimiter);
        byteBuffer.flip();
        channel.write(byteBuffer);
        byteBuffer.clear();
    }

    private void setByteBufferLimit(int length) throws Exception {
        if (length > currentByteBufferLimit) {
            throw new Exception("byteBufferLimit attempt shrink");
        }
        byteBuffer = ByteBuffer.allocate(length + 10);
        currentByteBufferLimit = length;
    }

    public void closeFile() throws IOException {
        channel.close();
    }

    private void readFile() throws IOException {
        RandomAccessFile reader = new RandomAccessFile(currentFileName, "r");
        while(true) {
            String s = reader.readLine();
            if (s == null) {
                break;
            }
            System.out.println(s);
        }
    }

//    public static void main(String[] args) throws Exception {
//        LogWrite write = new LogWrite();
//        var s = "this is fwhat is called value for-";
//        for (int i = 0; i < 20; i++) {
//            write.put((i + "").getBytes(StandardCharsets.UTF_8), (s + i).getBytes(StandardCharsets.UTF_8));
//        }
//        write.closeFile();
//        write.readFile();
//    }
}
