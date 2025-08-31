package io.github.theuntamed839.datastore4j.constants;

import java.nio.charset.StandardCharsets;

public enum Operations {
    WRITE,
    DELETE;

    private static final byte write = "W".getBytes(StandardCharsets.UTF_8)[0];
    private static final byte delete = "D".getBytes(StandardCharsets.UTF_8)[0];

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
