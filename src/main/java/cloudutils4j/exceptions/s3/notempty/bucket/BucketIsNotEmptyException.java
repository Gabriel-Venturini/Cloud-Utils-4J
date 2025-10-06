package cloudutils4j.exceptions.s3.notempty.bucket;

/**
 * Personalized exception that should happen when trying to delete a bucket, but it is not empty.
 */
public class BucketIsNotEmptyException extends RuntimeException {
    public BucketIsNotEmptyException(String message) { super(message); }
}
