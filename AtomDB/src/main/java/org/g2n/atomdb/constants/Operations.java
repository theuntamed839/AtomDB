package org.g2n.atomdb.constants;

import org.g2n.atomdb.util.BytesConverter;

public enum Operations {
    WRITE,
    DELETE;

    private static final byte write = BytesConverter.bytes("W")[0];
    private static final byte delete = BytesConverter.bytes("D")[0];

    public static Operations getOperation(byte op) {
        if (op == write) return WRITE;
        if (op == delete) return DELETE;
        throw new IllegalArgumentException("Not an operation type: " + op);
    }

    public byte value() {
        return switch (this) {
            case WRITE -> write;
            case DELETE -> delete;
        };
    }
}
