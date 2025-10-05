package cloudutils4j.exceptions.s3.invalid.bucket;

/**
 * Personalized exception that occurs when trying to create a bucket but the name is invalid.
 * Usually when using upper chars, special chars, etc.
 *
 * @author Gabriel Venturini
 */
public class InvalidBucketNameException extends RuntimeException {
    public InvalidBucketNameException(String message) { super(message); }
}
