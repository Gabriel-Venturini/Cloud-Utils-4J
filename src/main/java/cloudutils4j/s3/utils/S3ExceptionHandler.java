package cloudutils4j.s3.utils;

import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;

import cloudutils4j.exceptions.s3.io.StorageException;
import cloudutils4j.exceptions.s3.notfound.bucket.BucketDoesNotExistsException;
import cloudutils4j.exceptions.s3.notfound.object.ObjectNotFoundException;

/**
 * Utility class for centralizing the handling and mapping of AWS SDK S3 exceptions
 * to the custom exceptions used in the application layer.
 * This adheres to the DRY principle by eliminating repeated try-catch logic in operation methods.
 */
public final class S3ExceptionHandler {

    private S3ExceptionHandler() {}

    /**
     * Handles an S3Exception, translating it into the appropriate custom application exception.
     *
     * @param e The S3Exception thrown by the AWS SDK.
     * @param operationName The name of the operation that failed (e.g., "list files", "delete bucket").
     * @param resourceName The name of the resource involved (e.g., bucket name, key name).
     * @throws StorageException The mapped custom exception, depending on the S3 error type.
     */
    public static void handle(S3Exception e, String operationName, String resourceName) throws StorageException {

        String message = "Failed to " + operationName + " for resource " + resourceName + ".";

        if (e instanceof NoSuchBucketException) {
            throw new BucketDoesNotExistsException("Bucket not found during " + operationName + ": " + resourceName);
        } else if (e instanceof NoSuchKeyException) {
            throw new ObjectNotFoundException("Object not found during " + operationName + ": " + resourceName);
        } else if (e instanceof BucketAlreadyOwnedByYouException || e instanceof BucketAlreadyExistsException) {
            throw new StorageException("Bucket already exists: " + resourceName, e);
        } else {
            throw new StorageException(message + " S3Exception: " + e.getMessage(), e);
        }
    }

    /**
     * Handles generic exceptions that are not S3Exception (e.g., IOException, NullPointerException).
     *
     * @param e The generic Exception.
     * @param operationName The name of the operation that failed.
     * @throws StorageException Mapped as a generic storage error.
     */
    public static void handleUnknownError(Exception e, String operationName) throws StorageException {
        throw new StorageException("Failed to " + operationName + ". Unknown error: " + e.getMessage(), e);
    }
}
