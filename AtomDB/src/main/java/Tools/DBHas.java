package Tools;

import db.DBComparator;
import sst.Header;
import sst.MiddleBlock;
import sst.ValueUnit;
import util.BytesConverter;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * basically this class just takes a folder and a key
 * searches all files for that key
 * reports the file name if the key is found in it, either it is with deleted marker or not
 */

public class DBHas {

    private static final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);

    public static void main(String[] args) throws Exception {
        var file = new File("UpdationDeletionTestDB"); // db folder
        byte[] key = BytesConverter.bytes(32856 + ""); // key to search
        for (File listFile : file.listFiles()) {
            if (!listFile.getName().contains(".sst")) continue;
            ValueUnit found = trySearch(listFile.toPath().toString(), key);
            if (found == null) {
                System.out.println(listFile.getName()+" not found");
            } else {
                if (found.getIsDelete() == ValueUnit.DELETE) {
                    System.out.println(listFile.getName()+" found and was deleted");
                } else {
                    System.out.println(listFile.getName() + " found " + (new String(found.getValue())));
                }
            }
        }
    }

    private static ValueUnit trySearch(String file, byte[] key) throws Exception {
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

            List<Long> pointers = MiddleBlock.readPointers(channel,
                    byteBuffer,
                    header.getBinarySearchLocation(),
                    new ArrayList<>((int) header.getEntries()),
                    header.getEntries());

            header.close();

            Map.Entry<byte[], ValueUnit> entry = performBinarySearch(channel,
                    pointers, key);

            if (entry == null) {
                return null;
            } else {
                return entry.getValue();
            }
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
            int compare = DBComparator.byteArrayComparator.compare(foundKey, key);
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
