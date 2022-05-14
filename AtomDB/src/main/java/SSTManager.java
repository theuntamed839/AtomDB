import java.nio.ByteBuffer;
import java.util.Map;

public class SSTManager {
    private DBOptions dbOptions;
    private ByteBuffer byteBuffer;
    private int currentBufferSize = 4096;

    public SSTManager(DBOptions options) {
        this.dbOptions = options;
        byteBuffer = ByteBuffer.allocate(currentBufferSize);
    }

    public void process(Map<byte[], byte[]> map) {
        
    }
}
