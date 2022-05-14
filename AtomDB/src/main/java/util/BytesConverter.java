package util;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class BytesConverter {

    public static byte[] bytes(String value) {
        requireNonNull(value);
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
