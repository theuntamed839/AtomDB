import java.util.Arrays;
import java.util.Comparator;

public class DBComparator {
    public static final Comparator<byte[]> byteArrayComparator = Arrays::compare;
}
