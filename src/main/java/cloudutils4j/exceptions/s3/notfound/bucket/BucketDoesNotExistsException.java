package cloudutils4j.exceptions.s3.notfound.bucket;

/**
 * Personalized exception that occurs when trying to delete/find a bucket that do not exist.
 *
 * @author Gabriel Venturini
 */
public class BucketDoesNotExistsException extends RuntimeException {
    public BucketDoesNotExistsException(String message) { super(message); }
}
