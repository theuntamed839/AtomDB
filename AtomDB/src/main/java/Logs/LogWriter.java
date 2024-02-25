package Logs;

import Constants.Operations;

import java.io.IOException;

public interface LogWriter extends AutoCloseable {
    void logOP(byte[] key, byte[] value, Operations operations) throws Exception;
    String getLogFileName();
}
