package org.example.javaBug;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.ref.Cleaner;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Throwable {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        var invoker = MethodHandles.lookup()
                .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class))
                .bindTo(theUnsafe.get(null));

        MappedByteBuffer map = new RandomAccessFile(new File("C:\\Users\\GaneshNaik\\Documents\\a.txt"), "r")
                .getChannel().map(FileChannel.MapMode.READ_ONLY, 0, 100);
        invoker.invoke(map);
        map.getInt();
    }
}