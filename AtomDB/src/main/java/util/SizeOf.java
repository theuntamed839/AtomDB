package util;

import Constants.Operations;
public class SizeOf {
    public final static int LongLength =  Long.BYTES;
    public final static int ShortLength = Short.BYTES;
    public final static int IntLength = Integer.BYTES;
    public static final int MBinBytes = 1_000_000;
    public static final int InstantTimeLength = IntLength + LongLength;
    public static final int OperationsLength = Operations.bytesLength();
}
