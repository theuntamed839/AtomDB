import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public interface LogReader {
    void readWAL(Map<byte[], byte[]> map) throws Exception;
}
