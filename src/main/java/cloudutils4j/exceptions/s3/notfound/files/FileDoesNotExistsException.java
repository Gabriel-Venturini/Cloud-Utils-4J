package cloudutils4j.exceptions.s3.notfound.files;

/**
 * Personalized exception that occurs when trying to access a file that do not exist.
 *
 * @author Gabriel Venturini
 */
public class FileDoesNotExistsException extends RuntimeException {
    public FileDoesNotExistsException(String message) { super(message); }
}