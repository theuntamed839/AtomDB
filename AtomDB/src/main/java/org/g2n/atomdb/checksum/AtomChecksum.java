package org.g2n.atomdb.checksum;

public interface AtomChecksum {

    long compute(byte[] arr);

    long compute(byte[] key, byte[] value);
}
