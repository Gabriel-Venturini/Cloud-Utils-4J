package cloudutils4j.s3.utils;

import cloudutils4j.exceptions.s3.invalid.key.*;
import cloudutils4j.exceptions.s3.invalid.localpath.EmptyDestinationLocalPathException;
import cloudutils4j.exceptions.s3.invalid.localpath.NullDestinationLocalPathException;
import cloudutils4j.exceptions.s3.invalid.localpath.NullLocalPathException;
import cloudutils4j.exceptions.s3.invalid.prefix.NullPrefixException;

/**
 * Utility class dedicated to validating various parameters related to AWS S3 operations,
 * such as prefixes, keys (source and destination paths on S3), and local file paths.
 * <p>
 * This class primarily checks for null and empty string values for critical parameters
 * to prevent runtime errors during AWS SDK calls.
 * </p>
 *
 * @author Gabriel Venturini
 */
public class S3ParamValidator {

    private S3ParamValidator() {}

    /**
     * Delegates validation of a parameter based on its type.
     *
     * @param param The value of the parameter to validate.
     * @param paramType The type of the parameter (e.g., "prefix", "key").
     */
    public static void validate(String param, String paramType) {

        switch (paramType) {
            case "prefix":
                validatePrefix(param);
                break;
            case "key":
                validateKey(param);
                break;
            case "localPath":
                validateLocalPath(param);
                break;
            case "destinationKey":
                validateDestinationKey(param);
                break;
            case "sourceKey":
                validateSourceKey(param);
                break;
            case "localDestinationPath":
                validateLocalDestinationPath(param);
                break;
            default:
                // TODO: treat unknown parameters
                break;
        }
    }

    /**
     * Validates an S3 prefix parameter.
     *
     * @param prefix The S3 prefix value.
     * @throws NullPrefixException if the prefix is {@code null}.
     */
    private static void validatePrefix(String prefix) {
        if (prefix == null) {throw new NullPrefixException("Prefix cannot be a null value!");}
    }

    /**
     * Validates a generic S3 key (path) parameter.
     *
     * @param key The S3 object key value.
     * @throws NullKeyException if the key is {@code null}.
     * @throws EmptyKeyException if the key is an empty string.
     */
    private static void validateKey(String key) {
        if (key == null) {throw new NullKeyException("Key (path) cannot be a null value!");}
        if (key.isEmpty()) {throw new EmptyKeyException("Key (path) cannot be empty!");}
    }

    /**
     * Validates a local file path parameter, typically for the source file in an upload operation.
     *
     * @param localPath The local file system path.
     * @throws NullLocalPathException if the local path is {@code null}.
     * @throws EmptyDestinationLocalPathException if the local path is an empty string.
     */
    private static void validateLocalPath(String localPath) {
        if (localPath == null) {throw new NullLocalPathException("Local path cannot be a null value!");}
        // Note: Using EmptyDestinationLocalPathException here might be a naming inconsistency,
        // consider creating a generic EmptyLocalPathException if this is a generic source path.
        if (localPath.isEmpty()) {throw new EmptyDestinationLocalPathException("Local path cannot be empty!");}
    }

    /**
     * Validates a destination S3 key parameter (used in copy or move operations).
     *
     * @param destinationKey The destination S3 object key.
     * @throws NullDestinationKeyException if the destination key is {@code null}.
     * @throws EmptyDestinationKeyException if the destination key is an empty string.
     */
    private static void validateDestinationKey(String destinationKey) {
        if (destinationKey == null) {throw new NullDestinationKeyException("Destination key cannot be a null value!");}
        if (destinationKey.isEmpty()) {throw new EmptyDestinationKeyException("Destination key cannot be empty!");}
    }

    /**
     * Validates a source S3 key parameter (used in copy or move operations).
     *
     * @param sourceKey The source S3 object key.
     * @throws NullSourceKeyException if the source key is {@code null}.
     * @throws EmptySourceKeyException if the source key is an empty string.
     */
    private static void validateSourceKey(String sourceKey) {
        if (sourceKey == null) {throw new NullSourceKeyException("Source key cannot be empty!");}
        if (sourceKey.isEmpty()) {throw new EmptySourceKeyException("Source key cannot be empty!");}
    }

    /**
     * Validates a local destination path parameter (used in download operations).
     *
     * @param localDestinationPath The local file system path for the downloaded object.
     * @throws NullDestinationLocalPathException if the local destination path is {@code null}.
     * @throws EmptyDestinationLocalPathException if the local destination path is an empty string.
     */
    private static void validateLocalDestinationPath(String localDestinationPath) {
        if (localDestinationPath == null) {throw new NullDestinationLocalPathException("Local path cannot be a null value!");}
        if (localDestinationPath.isEmpty()) {throw new EmptyDestinationLocalPathException("Local Destination Path cannot be empty!");}
    }
}
