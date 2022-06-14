import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static util.BytesConverter.bytes;

public class TrashTest {
    @Test
    public void test() {
        Assertions.assertNull(null);
        Assertions.assertArrayEquals(bytes(12312 + ""), bytes(12312 + ""));
    }
}
