package se.irori.kroxylicious.filter.exception;

public class InvalidFileReferenceException extends RuntimeException {
    public InvalidFileReferenceException(String ref) {
        super("Invalid ref: " + ref);
    }
}
