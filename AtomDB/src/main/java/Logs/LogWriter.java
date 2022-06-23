package Logs;

import Constants.Operations;

import java.io.IOException;

public interface LogWriter {
    void logOP(byte[] key, byte[] value, Operations operations) throws Exception;

    void close() throws IOException;

    String getCurrentFileName();

    public void deleteAndCreateNewLogFile() throws IOException;
}
