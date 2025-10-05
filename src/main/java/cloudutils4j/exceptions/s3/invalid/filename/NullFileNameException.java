package cloudutils4j.exceptions.s3.invalid.filename;

/**
 * Personalized exception that happens when the file name provided is a null value.
 *
 * @author Gabriel Venturini
 */
public class NullFileNameException extends RuntimeException {
    public NullFileNameException(String message) { super(message); }
}
