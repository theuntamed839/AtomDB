package org.g2n.atomdb.util;

import java.util.stream.LongStream;

public class Verify {

    // google
    public static long googleFromBytes(
            byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
        return (b1 & 0xFFL) << 56
                | (b2 & 0xFFL) << 48
                | (b3 & 0xFFL) << 40
                | (b4 & 0xFFL) << 32
                | (b5 & 0xFFL) << 24
                | (b6 & 0xFFL) << 16
                | (b7 & 0xFFL) << 8
                | (b8 & 0xFFL);
    }


    // google
    public static byte[] googleToByteArray(long value) {
        // Note that this code needs to stay compatible with GWT, which has known
        // bugs when narrowing byte casts of long values occur.
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return result;
    }

    // hbase
    public static long hbaseToLong(byte[] bytes) {
        long l = 0;
        for (int i = 0; i < 8; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    //hbase
    public static byte[] hbaseToBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static long stackFromBytes(
            byte[] b) {
        return (b[7] & 0xffL) << 56
                | (b[6] & 0xffL) << 48
                | (b[5] & 0xffL) << 40
                | (b[4] & 0xffL) << 32
                | (b[3] & 0xffL) << 24
                | (b[2] & 0xffL) << 16
                | (b[1] & 0xffL) << 8
                | (b[0] & 0xffL);
    }

    public static byte[] stackToBytes(long lng) {
        byte[] b = new byte[]{
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)
        };
        return b;
    }

    public static void main(String[] args) {
        LongStream.range(Long.MIN_VALUE, Long.MAX_VALUE)
                .parallel()
                .filter(i -> {
                    byte[] bytes = googleToByteArray(i);
                    long l = googleFromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
                            bytes[5], bytes[6], bytes[7]);
                    return l != i;
                })
                .forEach(i -> System.out.println(i + " for google"));

        for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE ; i++) {
            //google
            byte[] bytes = googleToByteArray(i);
            long l = googleFromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
                    bytes[5], bytes[6], bytes[7]);
            if (l != i) {
                System.out.println("google for=" + i + " returned=" +l);
            }
            //stack
            byte[] bytes1 = stackToBytes(i);
            long l1 = stackFromBytes(bytes1);
            if (l1 != i) {
                System.out.println("stack for=" + i + " returned=" +l1);
            }
            //hbase
            byte[] bytes2 = hbaseToBytes(i);
            long l2 = hbaseToLong(bytes2);
            if (l2 != i) {
                System.out.println("hbase for=" + i + " returned=" +l2);
            }
        }
        System.out.println("done");
    }
}
