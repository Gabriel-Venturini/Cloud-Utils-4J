package cloudutils4j.exceptions.s3.conflict.bucket;

/**
 * Personalized exception that occurs when trying to create a bucket and it already exists.
 *
 * @author Gabriel Venturini
 */
public class BucketAlreadyExistsException extends RuntimeException {
    public BucketAlreadyExistsException(String message) { super(message); }
}
