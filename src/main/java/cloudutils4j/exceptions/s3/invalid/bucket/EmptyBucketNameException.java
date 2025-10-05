package cloudutils4j.exceptions.s3.invalid.bucket;

/**
 * Personalized exception that occurs when trying to create a bucket but the name parameter is empty.
 *
 * @author Gabriel Venturini
 */
public class EmptyBucketNameException extends RuntimeException {
    public EmptyBucketNameException(String message) { super(message); }
}
