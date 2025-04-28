package se.irori.kroxylicious.filter.exception;

public class PartitionProcessingException extends RuntimeException {
    public PartitionProcessingException(RuntimeException e) {
        super("Partition processing failed", e);
    }
}
