package cloudutils4j.exceptions.s3.invalid.key;

public class EmptySourceKeyException extends RuntimeException {
    public EmptySourceKeyException(String message) { super(message); }
}
