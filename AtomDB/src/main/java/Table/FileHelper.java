package Table;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import sst.Header;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileHelper {
    private static ByteBuffer buffer = ByteBuffer.allocateDirect(4096);

    public static BloomFilter<byte[]> readBloom(String pathToFile) {
        try(FileInputStream inputStream = new FileInputStream(pathToFile);
            FileChannel channel = inputStream.getChannel()) {
            Header header = Header.getHeader(pathToFile, channel, buffer);
            System.out.println(header);
            System.out.println(channel.size());
            long bloomStart = header.getBinarySearchLocation() + header.getEntries() * Long.BYTES;
            System.out.println("bloom read" + bloomStart);
            channel.position(bloomStart);
            BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(inputStream, Funnels.byteArrayFunnel());
            return bloomFilter;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
