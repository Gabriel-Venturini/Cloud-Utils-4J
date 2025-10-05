package cloudutils4j.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import cloudutils4j.exceptions.s3.notfound.bucket.BucketDoesNotExistsException;
import cloudutils4j.exceptions.s3.notfound.object.ObjectNotFoundException;
import cloudutils4j.exceptions.s3.io.StorageException;

/**
 * Defines a contract for operations with object storage services (S3-compatible).
 * <p>
 * This interface decouples the business logic from the specific implementation (e.g., AWS SDK),
 * focusing on the required actions rather than the underlying technology.
 *
 * @author Gabriel Venturini
 */
public interface StorageOperations {

    // --- Object Operations ---

    /**
     * Lists the objects within a bucket, optionally filtering by a prefix.
     *
     * @param bucketName The name of the bucket.
     * @param prefix The prefix (path/folder) to filter the results. Can be an empty string to list everything.
     * @return A list of keys (filenames) found.
     * @throws BucketDoesNotExistsException If the specified bucket does not exist.
     * @throws StorageException For other communication errors with the service.
     */
    List<String> listFiles(String bucketName, String prefix) throws BucketDoesNotExistsException, StorageException;

    /**
     * Checks if an object exists in the specified bucket.
     *
     * @param bucketName The name of the bucket.
     * @param key The key (full file path) of the object in the bucket.
     * @return {@code true} if the object exists, {@code false} otherwise.
     * @throws StorageException For other communication errors with the service.
     */
    boolean fileExists(String bucketName, String key) throws StorageException;

    /**
     * Uploads a file from the local filesystem to the storage service.
     *
     * @param localPath The full path of the source file on the local machine.
     * @param bucketName The name of the destination bucket.
     * @param destinationKey The key (full file path) of the destination in the bucket.
     * @throws BucketDoesNotExistsException If the destination bucket does not exist.
     * @throws StorageException For other errors (e.g., lack of permission, network error).
     * @throws IOException If an error occurs while reading the local file.
     */
    void uploadFile(String localPath, String bucketName, String destinationKey) throws BucketDoesNotExistsException, StorageException, IOException;

    /**
     * Downloads an object from the storage service to the local filesystem.
     *
     * @param bucketName The name of the source bucket.
     * @param sourceKey The key (full file path) of the object to be downloaded.
     * @param localDestinationPath The full destination path on the local machine.
     * @throws ObjectNotFoundException If the source object is not found.
     * @throws StorageException For other communication errors.
     * @throws IOException If an error occurs while writing the local file.
     */
    void downloadFile(String bucketName, String sourceKey, String localDestinationPath) throws ObjectNotFoundException, StorageException, IOException;

    /**
     * Deletes an object from a bucket.
     *
     * @param bucketName The name of the bucket.
     * @param key The key (full file path) of the object to be deleted.
     * @throws ObjectNotFoundException If the object is not found for deletion.
     * @throws StorageException For other errors.
     */
    void deleteFile(String bucketName, String key) throws ObjectNotFoundException, StorageException;

    /**
     * Copies an object from one location to another, server-side.
     *
     * @param sourceBucket The source bucket.
     * @param sourceKey The source key.
     * @param destBucket The destination bucket.
     * @param destKey The destination key.
     * @throws ObjectNotFoundException If the source object is not found.
     * @throws StorageException For other errors.
     */
    void copyFile(String sourceBucket, String sourceKey, String destBucket, String destKey) throws ObjectNotFoundException, StorageException;

    /**
     * Moves an object from one location to another, server-side (typically a copy followed by a delete).
     *
     * @param sourceBucket The source bucket.
     * @param sourceKey The source key.
     * @param destBucket The destination bucket.
     * @param destKey The destination key.
     * @throws ObjectNotFoundException If the source object is not found.
     * @throws StorageException For other errors.
     */
    void moveFile(String sourceBucket, String sourceKey, String destBucket, String destKey) throws ObjectNotFoundException, StorageException;

    /**
     * Retrieves metadata for a specific object.
     *
     * @param bucketName The name of the bucket.
     * @param key The key of the object to inspect.
     * @return A Map containing metadata (e.g., "Content-Length", "Last-Modified").
     * @throws ObjectNotFoundException If the object is not found.
     * @throws StorageException For other errors.
     */
    Map<String, String> getFileInfo(String bucketName, String key) throws ObjectNotFoundException, StorageException;


    // --- Bucket Operations ---

    /**
     * Lists all buckets available for the configured account.
     *
     * @return A list of bucket names.
     * @throws StorageException In case of communication failure.
     */
    List<String> listBuckets() throws StorageException;

    /**
     * Checks if a specific bucket exists.
     *
     * @param bucketName The name of the bucket to be verified.
     * @return {@code true} if the bucket exists, {@code false} otherwise.
     * @throws StorageException In case of communication failure.
     */
    boolean bucketExists(String bucketName) throws StorageException;

    /**
     * Creates a new bucket.
     *
     * @param bucketName The name of the bucket to be created.
     * @throws StorageException If the bucket already exists or another error occurs.
     */
    void createBucket(String bucketName) throws StorageException;

    /**
     * Deletes a bucket.
     *
     * @param bucketName The name of the bucket to be deleted.
     * @throws BucketDoesNotExistsException If the bucket does not exist.
     * @throws StorageException If the bucket is not empty or another error occurs.
     */
    void deleteBucket(String bucketName) throws BucketDoesNotExistsException, StorageException;
}