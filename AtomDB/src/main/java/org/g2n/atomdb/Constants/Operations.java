package org.g2n.atomdb.Constants;

import org.g2n.atomdb.util.BytesConverter;

public enum Operations {
    WRITE,
    READ,
    UPDATE,
    DELETE;

    private static final byte write = BytesConverter.bytes("W")[0];
    private static final byte read = BytesConverter.bytes("R")[0];
    private static final byte update = BytesConverter.bytes("U")[0];
    private static final byte delete = BytesConverter.bytes("D")[0];

    public static Operations getOperation(byte given) {
        if (given == write) return WRITE;
        if (given == delete) return DELETE;
        throw new RuntimeException("not of org.g2n.atomdb.Constants.Operations type");
    }

    public byte value() {
        return switch (this) {
            case WRITE -> write;
            case READ -> read;
            case UPDATE -> update;
            case DELETE -> delete;
        };
    }

    public static int bytesLength() {
        return Byte.BYTES;
    }
}
