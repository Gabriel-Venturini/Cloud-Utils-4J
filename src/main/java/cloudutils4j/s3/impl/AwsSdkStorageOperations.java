package cloudutils4j.s3.impl;

import cloudutils4j.core.StorageOperations;
import cloudutils4j.exceptions.s3.notfound.bucket.BucketDoesNotExistsException;
import cloudutils4j.exceptions.s3.notfound.object.ObjectNotFoundException;
import cloudutils4j.exceptions.s3.io.StorageException;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import cloudutils4j.exceptions.s3.notfound.files.FileDoesNotExistsException;
import cloudutils4j.s3.utils.S3BucketValidator;
import cloudutils4j.s3.utils.S3ClientBuilderFactory;
import cloudutils4j.s3.utils.S3ExceptionHandler;
import cloudutils4j.s3.utils.S3ParamValidator;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AwsSdkStorageOperations implements StorageOperations {

    private final S3Client s3;

    /**
     * Constructor to initialize the S3 client with specific credentials and endpoint.
     *
     * @param endpoint  The service endpoint (e.g., "<a href="http://localhost:9000">...</a>" for MinIO).
     * @param region    The service region (e.g., "us-east-1").
     * @param accessKey The access key for authentication.
     * @param secretKey The secret key for authentication.
     */
    public AwsSdkStorageOperations(String endpoint, String region, String accessKey, String secretKey) {
        this.s3 = S3ClientBuilderFactory.buildClient(endpoint, region, accessKey, secretKey);
    }

    /**
     * Constructor for testing purposes, allowing dependency injection of a mocked S3Client.
     *
     * @param s3Client The S3Client instance to be used.
     */
    public AwsSdkStorageOperations(S3Client s3Client) {
        this.s3 = s3Client;
    }

    // --- Auxiliary Methods for Validation and Error Handling ---

    /**
     * Runs all required parameter validations for a given operation.
     *
     * @param bucketName The bucket name to validate.
     * @param params A map where key=parameterValue and value=parameterType (e.g., "prefix").
     */
    private void runValidations(String bucketName, Map<String, String> params) {
        S3BucketValidator.validate(bucketName);
        params.forEach(S3ParamValidator::validate);
    }

    // --- StorageOperations Implementation ---

    @Override
    public List<String> listFiles(String bucketName, String prefix) throws StorageException {
        runValidations(bucketName, Collections.singletonMap(prefix, "prefix"));

        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response response = s3.listObjectsV2(request);
            return response.contents().stream()
                    .map(S3Object::key)
                    .collect(Collectors.toList());
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "list files", bucketName);
            return null;
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "list files");
            return null;
        }
    }

    // Method overloading to pass default values for the listFiles function if no value is provided
    public List<String> listFiles(String bucketName) throws StorageException {
        return listFiles(bucketName, "");
    }

    @Override
    public boolean fileExists(String bucketName, String key) throws StorageException {
        runValidations(bucketName, Collections.singletonMap(key, "key"));

        try {
            s3.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "check file existence", bucketName);
            return false;
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "check file existence");
            return false;
        }
    }

    @Override
    public void uploadFile(String localPath, String bucketName, String destinationKey) throws StorageException {
        Map<String, String> uploadParams = new HashMap<>();
        uploadParams.put(localPath, "localPath");
        uploadParams.put(destinationKey, "destinationKey");
        runValidations(bucketName, uploadParams);

        File file = new File(localPath);
        if (!file.exists() || !file.isFile()) {
            throw new FileDoesNotExistsException("File does not exist: " + localPath);
        }

        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(destinationKey)
                    .build();

            s3.putObject(request, RequestBody.fromFile(file));
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "upload file", bucketName);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "upload file");
        }
    }

    @Override
    public void downloadFile(String bucketName, String sourceKey, String localDestinationPath) throws ObjectNotFoundException, StorageException {
        Map<String, String> downloadParams = new HashMap<>();
        downloadParams.put(sourceKey, "sourceKey");
        downloadParams.put(localDestinationPath, "localDestinationPath");
        runValidations(bucketName, downloadParams);

        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(sourceKey)
                    .build();
            s3.getObject(request, ResponseTransformer.toFile(Paths.get(localDestinationPath)));
        } catch (NoSuchKeyException e) {
            throw new FileDoesNotExistsException("Object not found: " + sourceKey);
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "download file", bucketName);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "download file");
        }
    }

    @Override
    public void deleteFile(String bucketName, String key) throws StorageException {
        runValidations(bucketName, Collections.singletonMap(key, "key"));

        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3.deleteObject(request);
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "delete file", bucketName);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "delete file");
        }
    }

    // TODO: create the unit tests for this function
    @Override
    public void copyFile(String sourceBucket, String sourceKey, String destBucket, String destKey) throws ObjectNotFoundException, StorageException {
        try {
            CopyObjectRequest request = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(destBucket)
                    .destinationKey(destKey)
                    .build();
            s3.copyObject(request);
        } catch (NoSuchKeyException e) {
            throw new ObjectNotFoundException("Source object not found: " + sourceKey);
        } catch (S3Exception e) {
            throw new StorageException("Failed to copy file: " + e.getMessage(), e);
        }
    }

    // TODO: create the unit tests for this function
    @Override
    public void moveFile(String sourceBucket, String sourceKey, String destBucket, String destKey) throws ObjectNotFoundException, StorageException {
        copyFile(sourceBucket, sourceKey, destBucket, destKey);
        deleteFile(sourceBucket, sourceKey);
    }

    // TODO: create the unit tests for this function
    @Override
    public Map<String, String> getFileInfo(String bucketName, String key) throws ObjectNotFoundException, StorageException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
            HeadObjectResponse response = s3.headObject(request);

            Map<String, String> info = new HashMap<>();
            info.put("Content-Length", String.valueOf(response.contentLength()));
            info.put("Last-Modified", response.lastModified().toString());
            info.put("Content-Type", response.contentType());
            info.put("ETag", response.eTag());
            return info;
        } catch (NoSuchKeyException e) {
            throw new ObjectNotFoundException("Object not found: " + key);
        } catch (S3Exception e) {
            throw new StorageException("Failed to get file info: " + e.getMessage(), e);
        }
    }

    // TODO: create the unit tests for this function
    @Override
    public List<String> listBuckets() throws StorageException {
        try {
            return s3.listBuckets().buckets().stream()
                    .map(Bucket::name)
                    .collect(Collectors.toList());
        } catch (S3Exception e) {
            throw new StorageException("Failed to list buckets: " + e.getMessage(), e);
        }
    }

    // TODO: create the unit tests for this function
    @Override
    public boolean bucketExists(String bucketName) throws StorageException {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        } catch (S3Exception e) {
            throw new StorageException("Failed to check bucket existence: " + e.getMessage(), e);
        }
    }

    // TODO: create the unit tests for this function
    @Override
    public void createBucket(String bucketName) throws StorageException {
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
            throw new StorageException("Bucket already exists: " + bucketName, e);
        } catch (S3Exception e) {
            throw new StorageException("Failed to create bucket: " + e.getMessage(), e);
        }
    }

    // TODO: create the unit tests for this function
    @Override
    public void deleteBucket(String bucketName) throws StorageException {
        try {
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            throw new BucketDoesNotExistsException("Bucket not found: " + bucketName);
        } catch (S3Exception e) {
            throw new StorageException("Failed to delete bucket (it may not be empty): " + e.getMessage(), e);
        }
    }
}