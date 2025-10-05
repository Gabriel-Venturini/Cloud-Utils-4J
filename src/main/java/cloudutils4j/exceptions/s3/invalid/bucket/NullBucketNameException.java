package cloudutils4j.exceptions.s3.invalid.bucket;

/**
 * Personalized exception that occurs when the bucket name parameter is a null value.
 * Even though it is probably not going to happen because the parameter should be a String,
 * it is a good practice to keep this error handling around.
 *
 * @author Gabriel Venturini
 */
public class NullBucketNameException extends RuntimeException {
    public NullBucketNameException(String message) { super(message); }
}
