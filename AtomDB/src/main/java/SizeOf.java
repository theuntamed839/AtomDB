import com.google.common.primitives.Longs;
import util.BytesConverter;

import java.time.LocalDateTime;

public class SizeOf {
    public final static int LongLength =  8;
    public static final int MBinBytes = 1_000_000;

    public static final int DateTimeLength = BytesConverter.bytes(LocalDateTime.now().toString()).length;

    public static final int OperationsLength = Operations.bytesLength();
}
