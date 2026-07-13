package top.nontage.jsharedmem.exception;

public class MemoryFullException extends RuntimeException {
    public MemoryFullException(String message) {
        super(message);
    }
}