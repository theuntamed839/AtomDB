package Logs;

import db.DB;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public interface LogReader {
    void readWAL(DB db) throws Exception;
}
