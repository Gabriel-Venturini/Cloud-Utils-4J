package cloudutils4j.exceptions.s3.invalid.key;

public class EmptyKeyException extends RuntimeException {
    public EmptyKeyException(String message) { super(message); }
}
