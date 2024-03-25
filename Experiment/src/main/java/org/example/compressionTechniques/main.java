package org.example.compressionTechniques;

import net.jpountz.lz4.*;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class main {
    public static void main(String[] args) throws IOException {
        LZ4Factory lz4Factory = LZ4Factory.nativeInstance();
        LZ4Compressor lz4FastCompressor = lz4Factory.fastCompressor();
        LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();
        LZ4CompressorWithLength lz4CompressorWithLength = new LZ4CompressorWithLength(lz4FastCompressor);
        LZ4DecompressorWithLength lz4DecompressorWithLength = new LZ4DecompressorWithLength(decompressor);

        ByteBuffer allocate = ByteBuffer.allocate(1024);
        Random random = new Random();

        int kSize = random.nextInt(100, 200);
        allocate.putInt(kSize);
        byte[] key = new byte[kSize];
        random.nextBytes(key);
        allocate.put(key);
        int vSize = random.nextInt(300, 500);
        allocate.putInt(vSize);
        allocate.put("qwertyuiopasdfghjklzxcvbnm,1234567890-]'/[;.,".getBytes());
        allocate.putLong(random.nextLong());

        allocate.flip();
        byte[] bytes = new byte[allocate.limit()];
        allocate.get(bytes);
        byte[] lzCompression = lz4CompressorWithLength.compress(bytes);
        byte[] snappyCompression = Snappy.compress(bytes);
        System.out.println(Arrays.toString(bytes));
        System.out.println(Arrays.toString(lzCompression));
        System.out.println(Arrays.toString(snappyCompression));
        System.out.println(bytes.length);
        System.out.println(lzCompression.length);
        System.out.println(snappyCompression.length);
    }
}
