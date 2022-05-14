import java.io.IOException;

public interface LogWriter {
    void logOP(byte[] key, byte[] value, Operations operations) throws Exception;

    void close() throws IOException;
}
