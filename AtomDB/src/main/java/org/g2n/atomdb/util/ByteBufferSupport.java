package org.g2n.atomdb.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

// with the help of
// https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
//  https://github.com/dain/leveldb/blob/130db6965ebba2c19106c5355bee0c8dc59f57db/leveldb/src/main/java/org/iq80/leveldb/util/ByteBufferSupport.java
public final class ByteBufferSupport {
    private static final MethodHandle INVOKE_CLEANER;

    static {
        MethodHandle invoker;
        try {
            // todo check if our code works with java 8 or so... if it doesn't work then delete the catch block.
            // Java 9 added an invokeCleaner method to Unsafe to work around
            // module visibility issues for code that used to rely on DirectByteBuffer's cleaner()
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            invoker = MethodHandles.lookup()
                    .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class))
                    .bindTo(theUnsafe.get(null));
        }
        catch (Exception e) {
            // fall back to pre-java 9 compatible behavior
            try {
                Class<?> directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
                Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");

                Method cleanerMethod = directByteBufferClass.getDeclaredMethod("cleaner");
                cleanerMethod.setAccessible(true);
                MethodHandle getCleaner = MethodHandles.lookup().unreflect(cleanerMethod);

                Method cleanMethod = cleanerClass.getDeclaredMethod("clean");
                cleanerMethod.setAccessible(true);
                MethodHandle clean = MethodHandles.lookup().unreflect(cleanMethod);

                clean = MethodHandles.dropArguments(clean, 1, directByteBufferClass);
                invoker = MethodHandles.foldArguments(clean, getCleaner);
            }
            catch (Exception e1) {
                throw new AssertionError(e1);
            }
        }
        INVOKE_CLEANER = invoker;
    }

    private ByteBufferSupport() {
    }

    public static void unmap(MappedByteBuffer buffer) {
        try {
            INVOKE_CLEANER.invoke(buffer);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
