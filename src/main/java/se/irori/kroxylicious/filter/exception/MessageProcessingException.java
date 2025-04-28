package se.irori.kroxylicious.filter.exception;

public class MessageProcessingException extends RuntimeException {
    public MessageProcessingException() {
        super("Processing of messages failed");
    }
}
