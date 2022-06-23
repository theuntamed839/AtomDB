package Constants;

import util.BytesConverter;

import java.util.Arrays;

public enum Operations {
    WRITE,
    READ,
    UPDATE,
    DELETE;

    private static final byte[] write = BytesConverter.bytes("WRIT");
    private static final byte[] read = BytesConverter.bytes("READ");
    private static final byte[] update = BytesConverter.bytes("UPDA");
    private static final byte[] delete = BytesConverter.bytes("DELE");

    public static Operations getOP(byte[] array) throws Exception {
        if (Arrays.compare(array, write) == 0) return WRITE;
        if (Arrays.compare(array, read) == 0) return READ;
        if (Arrays.compare(array, update) == 0) return UPDATE;
        if (Arrays.compare(array, delete) == 0) return DELETE;
        throw new Exception("not of Constants.Operations type");
    }

    public byte[] value() {
        return switch (this) {
            case WRITE -> write;
            case READ -> read;
            case UPDATE -> update;
            case DELETE -> delete;
        };
    }

    public static int bytesLength() {
        if (read.length != write.length ||
                write.length != update.length ||
                update.length != delete.length) {
            throw new RuntimeException("operation byte length not same");
        }
        return read.length;
    }
}
