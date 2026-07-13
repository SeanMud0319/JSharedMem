package top.nontage.jsharedmem.exception;

public class SharedMemoryException extends RuntimeException {

    public SharedMemoryException(String message) {
        super(message);
    }

    public SharedMemoryException(String message, Throwable cause) {
        super(message, cause);
    }
}