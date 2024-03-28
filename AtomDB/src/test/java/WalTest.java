import db.DBImpl;
import db.DBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import sst.SSTManager;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.*;
import static util.BytesConverter.bytes;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WalTest.class)
public class WalTest {
    public static final String VALUE = "value".repeat(50);
    DBOptions opt ;
    DBImpl db;
    private String dbDirectory;

    @BeforeEach
    public void init() throws Exception {
        opt = new DBOptions();
        dbDirectory = this.getClass().getName() + "_" + Instant.now().getEpochSecond() + "_DB";
        File directory = new File(dbDirectory);
        if (!directory.mkdir()) {
            throw new RuntimeException("Folder cannot be created");
        }
        db = new DBImpl(directory, opt);
    }

    @AfterEach
    public void closingSession() throws Exception {
        db.close();db.destroy();
    }

    @Test
    public void writeQuitRead1000() throws Exception {
        var map = new HashMap<byte[], byte[]>();
        for (int i = 0; i < 10; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
        }

        db.close();

        db = new DBImpl(new File(dbDirectory), opt);

        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (i % 10 == 0) System.out.println("hey");
            i++;
            System.out.println(new String(entry.getKey()) + "->" + new String(entry.getValue()));
            Assertions.assertArrayEquals(db.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void writeDeleteQuitRead() throws Exception {
        this.db.close();

        SSTManager mock = mock(SSTManager.class);
        whenNew(SSTManager.class).withAnyArguments().thenReturn(mock);
        when(mock.search(any())).thenReturn(null);

        DBImpl db = new DBImpl(new File(dbDirectory), opt);

        var map = new HashMap<byte[], byte[]>();
        var list = new ArrayList<byte[]>();
        for (int i = 0; i < 1000; i++) {
            map.put(bytes(i + ""), bytes(i + "_" + VALUE));
            db.put(bytes(i + ""), bytes(i + "_" + VALUE));
            list.add(bytes(i + ""));
        }

        int i = 500;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            if (i < 0 ) break;
            db.delete(entry.getKey());
            map.remove(entry.getKey());
            i--;
        }

        db.close();
        db = new DBImpl(new File(dbDirectory), opt);

        for (byte[] bytes : list) {
            Assertions.assertArrayEquals(db.get(bytes), map.get(bytes));
        }
        this.db = db;
    }
}