package sstIo;

import java.nio.ByteBuffer;

public interface SSTReader extends AutoCloseable{
    ByteBuffer readSize(byte[] arr, long position, int length);
    ByteBuffer readSize(byte[] arr, int length);
}