package Checksum;

import Constants.Operations;
import db.KVUnit;

public interface AtomChecksum {

    long compute(byte[] arr);

    long compute(byte[] key, byte[] value);
}
