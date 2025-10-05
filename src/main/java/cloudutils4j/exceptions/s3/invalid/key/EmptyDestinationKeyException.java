package cloudutils4j.exceptions.s3.invalid.key;

public class EmptyDestinationKeyException extends RuntimeException {
    public EmptyDestinationKeyException(String message) { super(message); }
}
