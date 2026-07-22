package top.nontage.jsharedmem.core;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;
import com.sun.jna.win32.StdCallLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.nontage.jsharedmem.exception.SharedMemoryException;

import java.io.File;

public class NativeBridge {

    private static final Logger logger = LoggerFactory.getLogger(NativeBridge.class);
    private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");

    private static long mappedAddress = 0;
    private static long handle = -1;
    private static String currentName = null;
    private static long currentSize = 0;

    public interface Kernel32 extends StdCallLibrary {
        // loadLibrary is jna 4.x method, so it can support 4.x ~ 5.x
        Kernel32 INSTANCE = Native.loadLibrary("kernel32", Kernel32.class);
        long INVALID_HANDLE_VALUE = -1L;
        int PAGE_READWRITE = 0x04;
        int FILE_MAP_WRITE = 0x02;
        int FILE_MAP_READ = 0x04;

        long CreateFileMappingW(long hFile, Pointer lpAttributes, int flProtect,
                                int dwMaximumSizeHigh, int dwMaximumSizeLow, WString lpName);

        long OpenFileMappingW(int dwDesiredAccess, boolean bInheritHandle, WString lpName);

        long MapViewOfFile(long hFileMappingObject, int dwDesiredAccess,
                           int dwFileOffsetHigh, int dwFileOffsetLow, long dwNumberOfBytesToMap);

        boolean UnmapViewOfFile(Pointer lpBaseAddress);

        boolean CloseHandle(long hObject);
    }

    public interface LibRt extends Library {
        int O_CREAT = 64;
        int O_RDWR = 2;
        int O_EXCL = 0x0800;
        int PROT_READ = 1;
        int PROT_WRITE = 2;
        int MAP_SHARED = 1;

        int shm_open(String name, int oflag, int mode);
        int ftruncate(int fd, long length);
        long mmap(long addr, long length, int prot, int flags, int fd, long offset);
        int munmap(long addr, long length);
        int close(int fd);
    }

    private static LibRt libRt = null;

    private static synchronized LibRt getLibRt() {
        if (IS_WINDOWS) {
            throw new SharedMemoryException("LibRt should not be loaded on Windows");
        }
        if (libRt == null) {
            String[] candidates = {
                    "/usr/lib/x86_64-linux-gnu/librt.so.1",
                    "/usr/lib/librt.so.1",
                    "rt",
                    "c"
            };

            for (String lib : candidates) {
                try {
                    // loadLibrary is jna 4.x method, so it can support 4.x ~ 5.x
                    libRt = Native.loadLibrary(lib, LibRt.class);
                    logger.info("Loaded C library: {}", lib);
                    return libRt;
                } catch (UnsatisfiedLinkError | Exception e) {
                    logger.debug("Failed to load {}: {}", lib, e.getMessage());
                }
            }

            throw new SharedMemoryException(
                    "Failed to load any C library for shared memory. " +
                            "Please install librt or libc development package.\n" +
                            "On Ubuntu/Debian: sudo apt-get install libc6-dev\n" +
                            "On WSL: ensure /usr/lib/x86_64-linux-gnu/librt.so.1 exists"
            );
        }
        return libRt;
    }

    private static void validateWindowsSharedMemory(String name, long size) {
        try {
            Kernel32 k32 = Kernel32.INSTANCE;
            long hMap = k32.OpenFileMappingW(
                    Kernel32.FILE_MAP_READ,
                    false,
                    new WString(name)
            );
            if (hMap == 0) {
                return;
            }

            long addr = 0;
            try {
                addr = k32.MapViewOfFile(hMap, Kernel32.FILE_MAP_READ, 0, 0, 1024);
                if (addr == 0) {
                    throw new SharedMemoryException(
                            "Failed to map Windows shared memory header for validation"
                    );
                }

                int magic = UnsafeUtil.getInt(addr);
                if (magic != 0x4A534D) {
                    throw new SharedMemoryException(
                            String.format("Invalid magic number in Windows shared memory: 0x%x (expected 0x4A534D)", magic)
                    );
                }

                long totalSize = UnsafeUtil.getLong(addr + 8);
                if (totalSize != size) {
                    throw new SharedMemoryException(
                            String.format("Windows shared memory size mismatch: found %d, expected %d", totalSize, size)
                    );
                }

                logger.debug("Windows shared memory validation passed: name={}, size={}", name, size);

            } finally {
                if (addr != 0) {
                    k32.UnmapViewOfFile(new Pointer(addr));
                }
                k32.CloseHandle(hMap);
            }

        } catch (SharedMemoryException e) {
            throw e;
        } catch (Exception e) {
            throw new SharedMemoryException("Windows shared memory validation failed: " + e.getMessage(), e);
        }
    }

    private static void validateLinuxSharedMemory(String name, long size) {
        if (IS_WINDOWS) return;

        String shmPath = "/dev/shm/" + name;
        File shmFile = new File(shmPath);

        if (!shmFile.exists()) {
            return;
        }

        try {
            LibRt rt = getLibRt();
            int fd = rt.shm_open("/" + name, LibRt.O_RDWR, 438);

            if (fd < 0) {
                logger.warn("Cannot open existing shared memory for validation: {}", shmPath);
                return;
            }

            long addr = rt.mmap(0, 1024, LibRt.PROT_READ, LibRt.MAP_SHARED, fd, 0);
            if (addr != -1) {
                int magic = UnsafeUtil.getInt(addr);
                boolean valid = false;

                if (magic == 0x4A534D || Integer.reverseBytes(magic) == 0x4A534D) {
                    long totalSize = UnsafeUtil.getLong(addr + 8);
                    if (totalSize == size) {
                        valid = true;
                        logger.debug("Valid shared memory found: {}", shmPath);
                    } else {
                        logger.warn("Size mismatch: found {}, expected {}", totalSize, size);
                    }
                } else {
                    logger.warn("Invalid magic number: 0x{}", Integer.toHexString(magic));
                }

                rt.munmap(addr, 1024);

                if (!valid) {
                    throw new SharedMemoryException(
                            "Existing shared memory is invalid or size mismatch: " + shmPath +
                                    ". Please delete manually: rm -f " + shmPath
                    );
                }
            }
            rt.close(fd);

        } catch (SharedMemoryException e) {
            throw e;
        } catch (Exception e) {
            logger.warn("Error validating shared memory: {}", e.getMessage());
        }
    }

    public static synchronized long createOrOpen(String name, long size) {
        if (mappedAddress != 0 && currentName != null && currentName.equals(name)) {
            logger.debug("Reusing existing shared memory: {}", name);
            return mappedAddress;
        }

        if (mappedAddress != 0) {
            logger.warn("Closing existing connection before creating new: {}", currentName);
            close();
        }

        String safeName = name.replaceAll("[^a-zA-Z0-9_]", "_");

        if (!IS_WINDOWS) {
            validateLinuxSharedMemory(safeName, size);
        } else {
            validateWindowsSharedMemory(safeName, size);
        }

        try {
            long address;

            if (IS_WINDOWS) {
                address = openWindows(safeName, size);
                if (address == 0) {
                    address = createWindows(safeName, size);
                }
            } else {
                address = openLinux(safeName, size);
                if (address == 0) {
                    address = createLinux(safeName, size);
                }
            }

            if (address == 0) {
                throw new SharedMemoryException("Failed to map shared memory: " + name);
            }

            currentName = name;
            currentSize = size;
            mappedAddress = address;

            logger.info("Shared memory created/opened: name={}, address=0x{}, size={} bytes",
                    name, Long.toHexString(address), size);

            return address;

        } catch (SharedMemoryException e) {
            throw e;
        } catch (Exception e) {
            throw new SharedMemoryException("Failed to create/open shared memory: " + name, e);
        }
    }

    private static long createWindows(String name, long size) {
        Kernel32 k32 = Kernel32.INSTANCE;

        long hMap = k32.CreateFileMappingW(
                Kernel32.INVALID_HANDLE_VALUE,
                null,
                Kernel32.PAGE_READWRITE,
                (int) (size >>> 32),
                (int) (size & 0xFFFFFFFFL),
                new WString(name)
        );

        if (hMap == 0) {
            int error = Native.getLastError();
            throw new SharedMemoryException("CreateFileMappingW failed: " + error);
        }

        handle = hMap;

        long addr = k32.MapViewOfFile(
                hMap,
                Kernel32.FILE_MAP_WRITE,
                0, 0,
                size
        );

        if (addr == 0) {
            int error = Native.getLastError();
            k32.CloseHandle(hMap);
            handle = -1;
            throw new SharedMemoryException("MapViewOfFile failed: " + error);
        }

        logger.debug("Created Windows shared memory: {}", name);
        return addr;
    }

    private static long openWindows(String name, long size) {
        Kernel32 k32 = Kernel32.INSTANCE;

        long hMap = k32.OpenFileMappingW(
                Kernel32.FILE_MAP_WRITE | Kernel32.FILE_MAP_READ,
                false,
                new WString(name)
        );

        if (hMap == 0) {
            return 0;
        }

        handle = hMap;

        long addr = k32.MapViewOfFile(
                hMap,
                Kernel32.FILE_MAP_WRITE | Kernel32.FILE_MAP_READ,
                0, 0,
                size
        );

        if (addr == 0) {
            int error = Native.getLastError();
            k32.CloseHandle(hMap);
            handle = -1;
            throw new SharedMemoryException("MapViewOfFile (open) failed: " + error);
        }

        logger.debug("Opened Windows shared memory: {}", name);
        return addr;
    }

    private static void closeWindows() {
        Kernel32 k32 = Kernel32.INSTANCE;

        if (mappedAddress != 0) {
            k32.UnmapViewOfFile(new Pointer(mappedAddress));
            mappedAddress = 0;
        }

        if (handle != -1) {
            k32.CloseHandle(handle);
            handle = -1;
        }
    }

    private static long createLinux(String name, long size) {
        LibRt rt = getLibRt();

        String shmName = "/" + name;
        int fd = rt.shm_open(shmName, LibRt.O_CREAT | LibRt.O_RDWR | LibRt.O_EXCL, 438);

        if (fd < 0) {
            int errno = Native.getLastError();
            if (errno == 17) { // EEXIST
                logger.debug("Shared memory already exists: {}", shmName);
                return 0;
            }
            throw new SharedMemoryException(
                    String.format("shm_open create failed: name=%s, errno=%d", shmName, errno)
            );
        }

        int truncateResult = rt.ftruncate(fd, size);
        if (truncateResult != 0) {
            int errno = Native.getLastError();
            rt.close(fd);
            throw new SharedMemoryException(
                    String.format("ftruncate failed: size=%d, errno=%d", size, errno)
            );
        }

        handle = fd;

        long addr = rt.mmap(0, size, LibRt.PROT_READ | LibRt.PROT_WRITE, LibRt.MAP_SHARED, fd, 0);

        if (addr == -1) {
            int errno = Native.getLastError();
            rt.close(fd);
            handle = -1;
            throw new SharedMemoryException(
                    String.format("mmap failed: size=%d, errno=%d", size, errno)
            );
        }

        logger.info("Created Linux shared memory: {}, fd={}, addr=0x{}", shmName, fd, Long.toHexString(addr));
        return addr;
    }

    private static long openLinux(String name, long size) {
        LibRt rt = getLibRt();

        String shmName = "/" + name;
        int fd = rt.shm_open(shmName, LibRt.O_RDWR, 438);

        if (fd < 0) {
            int errno = Native.getLastError();
            if (errno == 2) { // ENOENT
                logger.debug("Shared memory does not exist: {}", shmName);
            } else {
                logger.warn("shm_open failed: name={}, errno={}", shmName, errno);
            }
            return 0;
        }

        handle = fd;

        long addr = rt.mmap(0, size, LibRt.PROT_READ | LibRt.PROT_WRITE, LibRt.MAP_SHARED, fd, 0);

        if (addr == -1) {
            int errno = Native.getLastError();
            rt.close(fd);
            handle = -1;
            throw new SharedMemoryException(
                    String.format("mmap (open) failed: size=%d, errno=%d", size, errno)
            );
        }

        logger.info("Opened Linux shared memory: {}, fd={}, addr=0x{}", shmName, fd, Long.toHexString(addr));
        return addr;
    }

    private static void closeLinux() {
        if (IS_WINDOWS) return;

        LibRt rt;
        try {
            rt = getLibRt();
        } catch (SharedMemoryException e) {
            logger.warn("Failed to get LibRt for cleanup: {}", e.getMessage());
            return;
        }

        if (mappedAddress != 0) {
            int result = rt.munmap(mappedAddress, currentSize);
            if (result != 0) {
                int errno = Native.getLastError();
                logger.warn("munmap failed: address=0x{}, size={}, errno={}",
                        Long.toHexString(mappedAddress), currentSize, errno);
            }
            mappedAddress = 0;
        }

        if (handle != -1) {
            int result = rt.close((int) handle);
            if (result != 0) {
                int errno = Native.getLastError();
                logger.warn("close failed: fd={}, errno={}", handle, errno);
            }
            handle = -1;
        }
    }

    public static synchronized void close() {
        if (mappedAddress == 0) {
            return;
        }

        try {
            if (IS_WINDOWS) {
                closeWindows();
            } else {
                closeLinux();
            }

            logger.info("Shared memory closed: {}", currentName);

        } catch (Exception e) {
            logger.error("Error closing shared memory", e);
        } finally {
            mappedAddress = 0;
            handle = -1;
            currentName = null;
            currentSize = 0;
        }
    }

    public static synchronized boolean isArenaExists(String name) {
        String safeName = name.replaceAll("[^a-zA-Z0-9_]", "_");

        try {
            if (IS_WINDOWS) {
                Kernel32 k32 = Kernel32.INSTANCE;
                long hMap = k32.OpenFileMappingW(
                        Kernel32.FILE_MAP_READ,
                        false,
                        new WString(safeName)
                );

                if (hMap == 0) {
                    return false;
                }

                k32.CloseHandle(hMap);
                return true;

            } else {
                LibRt rt = getLibRt();
                String shmName = "/" + safeName;
                int fd = rt.shm_open(shmName, LibRt.O_RDWR, 438);

                if (fd < 0) {
                    return false;
                }

                rt.close(fd);
                return true;
            }
        } catch (Exception e) {
            logger.debug("Failed to check if arena exists: {}", e.getMessage());
            return false;
        }
    }

    public static long getMappedAddress() {
        return mappedAddress;
    }

    public static String getCurrentName() {
        return currentName;
    }

    public static long getCurrentSize() {
        return currentSize;
    }

    public static boolean isConnected() {
        return mappedAddress != 0;
    }
}