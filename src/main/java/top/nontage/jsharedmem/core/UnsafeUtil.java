package top.nontage.jsharedmem.core;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public final class UnsafeUtil {

    private static final Unsafe UNSAFE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Unsafe instance", e);
        }
    }

    public static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    public static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    public static void putInt(long address, int value) {
        UNSAFE.putInt(address, value);
    }

    public static int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    public static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    public static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    public static void putBoolean(long address, boolean value) {
        UNSAFE.putByte(address, (byte) (value ? 1 : 0));
    }

    public static boolean getBoolean(long address) {
        return UNSAFE.getByte(address) != 0;
    }

    public static void copyMemory(long srcAddress, long destAddress, long bytes) {
        UNSAFE.copyMemory(srcAddress, destAddress, bytes);
    }

    public static void setMemory(long address, long bytes, byte value) {
        UNSAFE.setMemory(address, bytes, value);
    }

    public static long getAddress(Object obj, long offset) {
        return UNSAFE.getLong(obj, offset);
    }

    public static boolean compareAndSwapInt(Object o, long flagAddr, int i, int i1) {
        return UNSAFE.compareAndSwapInt(o, flagAddr, i, i1);
    }

    public static boolean compareAndSwapLong(Object o, long writeOffsetAddress, long writeOffset, long newWriteOffset) {
        return UNSAFE.compareAndSwapLong(o, writeOffsetAddress, writeOffset, newWriteOffset);
    }

    public static void loadFence() {
        UNSAFE.loadFence();
    }

    public static void storeFence() {
        UNSAFE.storeFence();
    }

    public static void fullFence() {
        UNSAFE.fullFence();
    }
}