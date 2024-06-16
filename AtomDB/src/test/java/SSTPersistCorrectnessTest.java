//import Checksum.Crc32cChecksum;
//import Compaction.Pointer;
//import Compaction.PointerList;
//import Compaction.SSTPersist;
//import Compression.Lz4Compression;
//import Constants.DBConstant;
//import Level.Level;
//import com.google.common.hash.BloomFilter;
//import com.google.common.hash.Funnels;
//import com.google.common.jimfs.Configuration;
//import com.google.common.jimfs.Jimfs;
//import db.DBComparator;
//import db.KVUnit;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import sstIo.SSTHeader;
//import sstIo.SSTKeyRange;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.RandomAccessFile;
//import java.nio.ByteBuffer;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.file.FileSystem;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//import java.util.stream.IntStream;
//import java.util.stream.LongStream;
//
//public class SSTPersistCorrectnessTest {
//    private File testFile;
//    private FileSystem fs;
//
//    @BeforeEach
//    void setUp() throws IOException {
//        this.fs = Jimfs.newFileSystem(Configuration.unix());
//        Path pathToStore = fs.getPath("");
//        Path filePath = pathToStore.resolve("testFile.sst");
//        Files.createFile(filePath);
//        this.testFile = new File("testFile.sst");
//    }
//
//    @AfterEach
//    void tearDown() throws IOException {
//        fs.close();
//    }
//
//    @Test
//    void headerCorrectnessTest() {
//        //stage
//        int numberOfEntries = 50000;
//        var kvs = getListFilledWithKVs(numberOfEntries);
//        kvs.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getKey(), b.getKey()));
//
//        // suuuuutt
//        new SSTPersist(testFile, kvs.iterator(),
//                new SSTKeyRange(kvs.getSmallest().getKey(), kvs.getGreatest().getKey()),
//                numberOfEntries, 10);
//
//        // verification
//        SSTHeader header = getHeader(testFile);
//        Assertions.assertEquals(DBConstant.SST_VERSION, header.getSstVersion());
//        Assertions.assertEquals(Level.LEVEL_ZERO, header.getLevel());
//        Assertions.assertEquals(DBConstant.CRC32C_CHECKSUM_TYPE, header.getChecksumType());
//        Assertions.assertEquals(DBConstant.LZ4_COMPRESSION_TYPE, header.getCompressionType());
//        Assertions.assertEquals(DBConstant.CLUSTER_SIZE, header.getClusterKeyCount());
//        Assertions.assertEquals(DBConstant.SHORTEST_COMMON_PREFIX_USED, header.getShortestCommonPrefixUsed());
//        Assertions.assertEquals(numberOfEntries, header.getNumberOfEntries());
//        Assertions.assertArrayEquals(kvs.getSmallest().getKey(), header.getSstKeyRange().getSmallest());
//        Assertions.assertArrayEquals(kvs.getGreatest().getKey(), header.getSstKeyRange().getGreatest());
//    }
//
//    @Test
//    void filterCorrectnessTest() {
//        int numberOfEntries = 50000;
//        var kvs = getListFilledWithKVs(numberOfEntries);
//        kvs.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getKey(), b.getKey()));
//
//        // suuuuutt
//        new SSTPersist(testFile, kvs.iterator(),
//                new SSTKeyRange(kvs.getSmallest().getKey(), kvs.getGreatest().getKey()),
//                numberOfEntries, 10);
//        SSTHeader header = getHeader(testFile);
//
//        var filter = getFilter(testFile, header.getFilterPosition(), header.getPointersPosition());
//        long count = kvs.stream().filter(each -> filter.mightContain(each.getKey())).count();
//        Assertions.assertEquals(count, numberOfEntries);
//    }
//
//    @Test
//    void pointerCorrectnessTest() {
//        int numberOfEntries = 50000;
//        var kvs = getListFilledWithKVs(numberOfEntries);
//        kvs.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getKey(), b.getKey()));
//
//        // suuuuutt
//        var pointers = new SSTPersist(testFile, kvs.iterator(),
//                new SSTKeyRange(kvs.getSmallest().getKey(), kvs.getGreatest().getKey()),
//                numberOfEntries, 10).getCheckPoints();
//        SSTHeader header = getHeader(testFile);
//
//        var retrievedPointers = getPointersList(testFile, header.getPointersPosition(), header.getNumberOfEntries());
//        Assertions.assertEquals(pointers.size(), retrievedPointers.size());
//        IntStream.range(0, pointers.size()).forEach(each -> {
//            Assertions.assertEquals(pointers.get(each), retrievedPointers.get(each));
//        });
//    }
//
//    @Test
//    public void readAllValueTest() {
//        int numberOfEntries = 50000;
//        var kvs = getListFilledWithKVs(numberOfEntries);
//        kvs.sort((a, b) -> DBComparator.byteArrayComparator.compare(a.getKey(), b.getKey()));
//
//        // suuuuutt
//        var pointers = new SSTPersist(testFile, kvs.iterator(),
//                new SSTKeyRange(kvs.getSmallest().getKey(), kvs.getGreatest().getKey()),
//                numberOfEntries, 10).getCheckPoints();
//        SSTHeader header = getHeader(testFile);
//
//        pointers.add(new Pointer(header.getSstKeyRange().getGreatest(), -1));
//        for (KVUnit kv : kvs) {
//            Assertions.assertArrayEquals(kv.getValue(), findKey(testFile, kv.getKey(), pointers));
//        }
//    }
//
//    private byte[] findKey(File testFile, byte[] key, PointerList pointers) {
//        try (
//                RandomAccessFile randomAccessFile = new RandomAccessFile(testFile, "r");
//                FileChannel channel = randomAccessFile.getChannel();
//        ) {
//            var map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
//            int index = getCluster(pointers, key);
//            if (index == pointers.size() -1) {
//                index--; // finding the last element.
//            }
//            assert DBComparator.byteArrayComparator.compare(pointers.get(index).key(), key) <= 0;
//            return findKeyInCluster(map, pointers.get(index), key);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private byte[] findKeyInCluster(MappedByteBuffer map, Pointer point,byte[] key) throws IOException {
//        int NOT_FOUND_YET = -1;
//        map.position((int) point.position());
//        Crc32cChecksum crc32cChecksum = new Crc32cChecksum();
//        long keyChecksum = crc32cChecksum.compute(key);
//        int index = NOT_FOUND_YET;
//        for (int i = 0; i < DBConstant.CLUSTER_SIZE; i++) {
//            if (keyChecksum == map.getLong()) {
//                Assertions.assertEquals(index, NOT_FOUND_YET);
//                index = i;
//            }
//        }
//        map.position((int) point.position() + Long.BYTES * DBConstant.CLUSTER_SIZE + index * Integer.BYTES);
//        int keyLocation = map.getInt();
//        int nextKeyLocation = map.getInt();
//        map.position((int) point.position() + Long.BYTES * DBConstant.CLUSTER_SIZE + (DBConstant.CLUSTER_SIZE + 1) * Integer.BYTES);
//        int commonPrefix = map.getInt();
//        map.position((int) (point.position() +
//                        Long.BYTES * DBConstant.CLUSTER_SIZE +
//                        (DBConstant.CLUSTER_SIZE + 1) * Integer.BYTES
//                        + Integer.BYTES + keyLocation));
//
//        int blockSizeToRead = nextKeyLocation - keyLocation;
//        var block = new byte[blockSizeToRead];
//        map.get(block);
//        byte[] decompress = Lz4Compression.getInstance().decompress(block);
//        var wrap = ByteBuffer.wrap(decompress);
//        int keyLength = wrap.getInt();
//        wrap.position(wrap.position() + keyLength);
//        byte isDeleted = wrap.get();
//        if (isDeleted == KVUnit.DELETE) return null;
//
//        int valueLength = wrap.getInt();
//        byte[] bytes = new byte[valueLength];
//        wrap.get(bytes);
//        return bytes;
//    }
//
//    private int getCluster(PointerList pointers, byte[] key) {
//        int l = 0, h = pointers.size() - 1;
//        while(l <= h) {
//            int mid = (l + h) >>> 1;
//            Pointer midPointer = pointers.get(mid);
//            int compare = DBComparator.byteArrayComparator.compare(midPointer.key(), key);
//            if (compare < 0){
//                l = mid + 1;
//            }
//            else if (compare > 0) {
//                h = mid - 1;
//            }
//            else {
//                return mid;
//            }
//        }
//        return h;
//    }
//
//
//    private List<Pointer> getPointersList(File testFile, long pointersPosition, long numberOfEntries) {
//        var list = new ArrayList<Pointer>();
//        try (
//                RandomAccessFile randomAccessFile = new RandomAccessFile(testFile, "r");
//                FileChannel channel = randomAccessFile.getChannel();
//        ) {
//            var map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
//            map.position((int) pointersPosition);
//            LongStream.range(0L, (long) Math.ceil(numberOfEntries*1.0/DBConstant.CLUSTER_SIZE))
//                    .forEach(each -> list.add(Pointer.readBytesToObj(map)));
//
//            Assertions.assertEquals(DBConstant.MARK_FILE_END, map.getLong()); // end verification
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        return list;
//    }
//
//    private BloomFilter<byte[]> getFilter(File file, long filterPosition, long pointerPosition) {
//        try (
//                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
//                FileChannel channel = randomAccessFile.getChannel();
//        ) {
//            channel.position((int) filterPosition);
//            ByteBuffer buffer = ByteBuffer.allocate((int) (pointerPosition - filterPosition));
//            channel.read(buffer);
//            buffer.flip();
//            InputStream hey = new InputStream() {
//                @Override
//                public int read() throws IOException {
//                    return buffer.get();
//                }
//
//                @Override
//                public int read(byte[] b) throws IOException {
//                    int position = buffer.position();
//                    buffer.get(b);
//                    return buffer.position() - position;
//                }
//
//                @Override
//                public int read(byte[] b, int off, int len) throws IOException {
//                    int position = buffer.position();
//                    buffer.get(b, off, len);
//                    return buffer.position() - position;
//                }
//            };
//            BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(hey, Funnels.byteArrayFunnel());
//            assert channel.position() == pointerPosition;
//            return bloomFilter;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public SSTHeader getHeader(File file) {
//        try (
//                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
//                FileChannel channel = randomAccessFile.getChannel();
//        ) {
//            var map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
//
//            int headerSize = map.getInt();
//            var HEADER_START_POSITION = map.position();
//            map.limit(Integer.BYTES + headerSize); // lets limit it so that it throws an exception
//            var SST_VERSION = map.get();
//            var LEVEL = Level.fromID(map.get());
//            var CHECKSUM_TYPE = map.get();
//            var COMPRESSION_TYPE =map.get();
//            var CLUSTER_SIZE = map.get();
//            var SHORTEST_COMMON_PREFIX = map.get();
//            var numberOfEntries = map.getLong();
//            var filterPosition = map.getLong();
//            var pointerPosition = map.getLong();
//
//            var sstKeyRange = getSSTKeyRange(HEADER_START_POSITION + SSTHeader.SST_KEY_RANGE_POSITION_IN_HEADER, map);
//            return new SSTHeader(SST_VERSION, LEVEL, CHECKSUM_TYPE, COMPRESSION_TYPE, CLUSTER_SIZE, SHORTEST_COMMON_PREFIX,
//                    numberOfEntries, filterPosition, pointerPosition, sstKeyRange);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private static SSTKeyRange getSSTKeyRange(long position, MappedByteBuffer map) {
//        map.position((int) position);
//        byte[] firstKey = new byte[map.getInt()];
//        map.get(firstKey);
//        byte[] lastKey = new byte[map.getInt()];
//        map.get(lastKey);
//        return new SSTKeyRange(firstKey, lastKey, map.getLong());
//    }
//
//    public List<KVUnit> getListFilledWithKVs(int numberOfEntries) {
//        List<KVUnit> kvs = new ArrayList<>(50000);
//        Random random = new Random();
//        for (int i = 0; i < numberOfEntries; i++) {
//            byte[] key = new byte[1024];
//            byte[] value = new byte[1024];
//            random.nextBytes(key);
//            random.nextBytes(value);
//            kvs.add(new KVUnit(key, value));
//        }
//        return kvs;
//    }
//}
