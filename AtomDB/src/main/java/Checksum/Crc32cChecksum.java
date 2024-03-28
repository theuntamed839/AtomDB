package Checksum;

import java.util.zip.CRC32C;
public class Crc32cChecksum implements Checksum {
    private final CRC32C crc32c = new CRC32C();
    private long computeCheckSum(byte[] arr) {
        crc32c.reset();
        crc32c.update(arr);
        return crc32c.getValue();
    }

    private long computeCheckSum(byte arr) {
        crc32c.reset();
        crc32c.update(arr);
        return crc32c.getValue();
    }

    @Override
    public long logBlock(byte operation,
                         byte[] key, byte[] value) {
        crc32c.reset();
        crc32c.update(operation);
        crc32c.update(key);
        crc32c.update(value);
        return crc32c.getValue();
    }

    @Override
    public long compute(byte[] arr) {
        crc32c.reset();
        crc32c.update(arr);
        return crc32c.getValue();
    }

    @Override
    public long compute(byte[] key, byte[] value) {
        crc32c.reset();
        crc32c.update(key);
        crc32c.update(value);
        return crc32c.getValue();
    }
}
