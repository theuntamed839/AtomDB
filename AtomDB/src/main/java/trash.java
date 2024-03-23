import net.jpountz.lz4.*;
import org.apache.logging.log4j.core.util.Assert;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class trash {
    public static void main(String[] args) throws Exception {
        int numberOfChecksums = 1000;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * numberOfChecksums);
        Random random = new Random();
        for (int i = 0; i < numberOfChecksums; i++) {
            buffer.putLong(random.nextLong());
        }
        buffer.flip();
        var allChecksums = new byte[buffer.limit()];
        System.out.println("buffer filled="+buffer.limit());
        buffer.get(allChecksums);
        byte[] compress = Snappy.compress(allChecksums);
        System.out.println("uncompressed size="+allChecksums.length);
        System.out.println("compressed size="+compress.length);
        assert Arrays.compare(allChecksums, Snappy.uncompress(compress)) == 0;

        LZ4Factory lz4Factory = LZ4Factory.nativeInstance();
        LZ4Compressor lz4FastCompressor = lz4Factory.fastCompressor();
        LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

        Benchmark(lz4FastCompressor, decompressor, 10);
        Benchmark(lz4FastCompressor, decompressor, 1000);
        Benchmark(lz4FastCompressor, decompressor, 10000);
        Benchmark(lz4FastCompressor, decompressor, 100000);
        Benchmark(lz4FastCompressor, decompressor, 1000000);
        Benchmark(lz4FastCompressor, decompressor, 10000000);

//        String generaatedString = new String(array, StandardCharsets.UTF_8);
//        System.out.println(generaatedString);
//        var a = generaatedString.getBytes(StandardCharsets.UTF_8);
//        System.out.println("a original="+a.length);
//        System.out.println("a snappy="+ Snappy.compress(a).length);
//        System.out.println("a lsz="+lz4Compressor.compress(a).length);
    }

    private static void Benchmark(LZ4Compressor lz4FastCompressor, LZ4FastDecompressor decompressor, int n) throws IOException {
        List<byte[]> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            byte[] array = new byte[100];
            new Random().nextBytes(array);
            values.add(array);
        }
        // snappy
        long start, end;
        start = System.nanoTime();
        for (byte[] value : values) {
            byte[] compress1 = Snappy.compress(value);
            byte[] uncompress = Snappy.uncompress(compress1);
            assert Arrays.compare(value, uncompress) == 0;
        }
        end = System.nanoTime();
        System.out.println("snappy took for encode and decode="+(end - start));

        // LZ4
        start = System.nanoTime();
        LZ4CompressorWithLength lz4CompressorWithLength = new LZ4CompressorWithLength(lz4FastCompressor);
        LZ4DecompressorWithLength lz4DecompressorWithLength = new LZ4DecompressorWithLength(decompressor);
        for (byte[] value : values) {
            byte[] compress1 = lz4CompressorWithLength.compress(value);
            byte[] decompress = lz4DecompressorWithLength.decompress(compress1);
            assert Arrays.compare(value, decompress) == 0;
        }
        end = System.nanoTime();
        System.out.println("lz4 took for encode and decode="+(end - start));
    }
}
/**
 * a original=943
 * a snappy=654
 * a lsz=675
 */