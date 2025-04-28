package se.irori.kroxylicious.filter.exception;

public class FetchResponseProcessingException extends RuntimeException {
    public FetchResponseProcessingException(RuntimeException e) {
        super("Fetch filtering failed", e);
    }
}
