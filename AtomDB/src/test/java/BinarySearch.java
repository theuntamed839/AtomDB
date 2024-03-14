import sst.Header;
import sst.MiddleBlock;
import sst.ValueUnit;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BinarySearch {
    static ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);

    public static ValueUnit file(String file, byte[] key) throws Exception {
        try(FileInputStream inputStream = new FileInputStream(file);
            FileChannel channel = inputStream.getChannel();
        ) {
            Header header = null; //Header.getHeader(file, channel, byteBuffer);

            // bound check
            if (Arrays.compare(key, header.getSmallestKey()) < 0) {
                return null;
            }
            if (Arrays.compare(key, header.getLargestKey()) > 0) {
                return null;
            }

            // debug
            System.out.println(header);

            List<Long> pointers = MiddleBlock.readPointers(channel,
                    byteBuffer,
                    header.getBinarySearchLocation(),
                    new ArrayList<>((int) header.getEntries()),
                    header.getEntries());

            header.close();

            Map.Entry<byte[], ValueUnit> entry = performBinarySearch(channel,
                    pointers, key);
            if (entry == null) return null;
            return entry.getValue();
        }catch (Exception e) {
            throw new RuntimeException("while accessing file=" + file, e);
        }
    }

    private static Map.Entry<byte[], ValueUnit> performBinarySearch(FileChannel channel, List<Long> pointers, byte[] key) throws Exception {
        int l = 0, h = pointers.size() -1;
        while(l <= h) {
            int mid = (l + h) >>> 1;
            Long offset = pointers.get(mid);
            byte[] foundKey = MiddleBlock.readKey(channel, byteBuffer, offset);
            int compare = Arrays.compare(foundKey, key);
            if (compare < 0)
                l = mid + 1;
            else if (compare > 0)
                h = mid - 1;
            else
                //todo can be improved
                return MiddleBlock.readKeyValue(channel, byteBuffer, offset);
        }
        return null;
    }
}
