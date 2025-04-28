package se.irori.kroxylicious.filter.exception;

public class PersistFailedException extends RuntimeException {
    public PersistFailedException() {
        super("Failed to persist oversize message");
    }
}
