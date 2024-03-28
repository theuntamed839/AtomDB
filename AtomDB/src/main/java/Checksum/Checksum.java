package Checksum;

public interface Checksum {
    long logBlock(byte operation,
                  byte[] key, byte[] value);

    long compute(byte[] arr);

    long compute(byte[] key, byte[] value);
}
