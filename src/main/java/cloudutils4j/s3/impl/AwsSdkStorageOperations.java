package cloudutils4j.s3.impl;

import cloudutils4j.core.StorageOperations;
import cloudutils4j.exceptions.s3.io.StorageException;

import cloudutils4j.exceptions.s3.notfound.bucket.BucketDoesNotExistsException;
import cloudutils4j.s3.utils.*;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import cloudutils4j.exceptions.s3.notfound.files.FileDoesNotExistsException;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

/**
 * Implementation of {@link StorageOperations} using AWS SDK for S3.
 *
 * <p>
 * Provides synchronous and asynchronous methods for common storage operations including:
 * <ul>
 *     <li>Uploading and downloading files (both sync and async)</li>
 *     <li>Listing files and buckets</li>
 *     <li>Checking existence, deleting, copying, and moving files</li>
 *     <li>Retrieving file metadata</li>
 * </ul>
 *
 * <p>
 * Asynchronous file transfers leverage {@link S3AsyncFileTransfer} to report progress in increments of 5%.
 * The progress of ongoing transfers can be monitored via {@link #getStatusPercentage()}.
 *
 * <p>
 * This implementation ensures parameter validation, proper exception handling, and supports dependency
 * injection for testing with mocked S3 clients.
 *
 * <p>
 * I hope you enjoy my code - but I also hope that you dislike it enough to want to improve it.
 * <p/>
 * As Isaac Newton once wrote to his rival Robert Hooke:
 * <blockquote>"If I have seen further, it is by standing on the shoulders of giants."</blockquote>
 *
 * <p>
 * Author: Gabriel Venturini
 */
public class AwsSdkStorageOperations implements StorageOperations {

    private final S3Client s3;
    private final S3AsyncClient s3Async;
    private final S3AsyncFileTransfer s3AsyncFileTransfer;
    private long statusPercentage = 0;

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
        this.s3Async = S3ClientBuilderFactory.buildAsyncClient(endpoint, region, accessKey, secretKey);
        this.s3AsyncFileTransfer = new S3AsyncFileTransfer();
        this.s3AsyncFileTransfer.setProgressConsumer(p -> this.statusPercentage = p);
    }

    /**
     * Constructor with both synchronous and asynchronous clients, which allow dependency injection for both mocked S3Client and S3AsyncClient.
     *
     * @param s3Client The S3Client instance to be used for synchronous operations.
     * @param s3AsyncClient The S3AsyncClient instance to be used for asynchronous operations.
     * @param s3AsyncFileTransfer The S3AsyncFileTransfer instance to be used for asynchronous operations.
     */
    public AwsSdkStorageOperations(S3Client s3Client, S3AsyncClient s3AsyncClient, S3AsyncFileTransfer s3AsyncFileTransfer) {
        this.s3 = s3Client;
        this.s3Async = s3AsyncClient;
        this.s3AsyncFileTransfer = s3AsyncFileTransfer;

        if (this.s3AsyncFileTransfer != null) {
            this.s3AsyncFileTransfer.setProgressConsumer(p -> this.statusPercentage = p);
        }
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

    public long getStatusPercentage() {
        return this.statusPercentage;
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

            List<String> allKeys = new ArrayList<>();

            s3.listObjectsV2Paginator(request).stream()
                    .flatMap(resp -> resp.contents().stream())
                    .forEach(s3Object -> allKeys.add(s3Object.key()));

            return allKeys;
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "list files", bucketName);
            return null;
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "list files");
            return null;
        }
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

    public CompletableFuture<PutObjectResponse> uploadFileAsync(String localPath, String bucketName, String destinationKey) throws StorageException {
        Map<String, String> uploadParams = new HashMap<>();
        uploadParams.put(localPath, "localPath");
        uploadParams.put(destinationKey, "destinationKey");
        runValidations(bucketName, uploadParams);

        File file = new File(localPath);

        if (!file.exists() || !file.isFile()) {
            throw new FileDoesNotExistsException("File does not exist: " + localPath);
        }

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(destinationKey)
                .checksumAlgorithm(ChecksumAlgorithm.CRC32)
                .build();

        return s3Async.putObject(request, s3AsyncFileTransfer.uploadFromFile(Paths.get(localPath)))
                .handle((result, ex) -> {
                    if (ex != null) {
                        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;

                        if (cause instanceof NoSuchBucketException) {
                            throw new BucketDoesNotExistsException("Bucket not found: " + bucketName);
                        } else if (cause instanceof S3Exception) {
                            S3Exception s3Ex = (S3Exception) cause;
                            try {
                                S3ExceptionHandler.handle(s3Ex, "upload file", bucketName);
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                            // Fallback
                            throw new RuntimeException(new StorageException(s3Ex.getMessage(), s3Ex));
                        } else {
                            try {
                                S3ExceptionHandler.handleUnknownError((Exception) cause, "upload file");
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                            throw new RuntimeException(new StorageException(cause.getMessage(), cause));
                        }
                    }
                    return result;
                });
    }

    @Override
    public void downloadFile(String bucketName, String sourceKey, String localDestinationPath) throws StorageException {
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

    public CompletableFuture<Path> downloadFileAsync(String bucketName, String sourceKey, String localDestinationPath) {
        Map<String, String> downloadParams = new HashMap<>();
        downloadParams.put(sourceKey, "sourceKey");
        downloadParams.put(localDestinationPath, "localDestinationPath");
        runValidations(bucketName, downloadParams);

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(sourceKey)
                .checksumMode(ChecksumMode.ENABLED) // The SDK provides an automatic checksum verification
                .build();

        return s3Async.getObject(request, s3AsyncFileTransfer.downloadToFile(Paths.get(localDestinationPath)))
                .handle((result, ex) -> {
                    if (ex != null) {
                        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;

                        if (cause instanceof NoSuchKeyException) {
                            throw new FileDoesNotExistsException("Object not found: " + sourceKey);
                        } else if (cause instanceof NoSuchBucketException) {
                            throw new BucketDoesNotExistsException("Bucket not found: " + bucketName);
                        } else if (cause instanceof S3Exception) {
                            S3Exception s3Ex = (S3Exception) cause;
                            try {
                                S3ExceptionHandler.handle(s3Ex, "download file", bucketName);
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                            try {
                                throw new StorageException(s3Ex.getMessage(), s3Ex);
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            try {
                                S3ExceptionHandler.handleUnknownError((Exception) cause, "download file");
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                            try {
                                throw new StorageException(cause.getMessage(), cause);
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    return result;
                });
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

    @Override
    public void copyFile(String sourceBucket, String sourceKey, String destBucket, String destKey) throws StorageException {
        S3BucketValidator.validate(sourceBucket);
        S3BucketValidator.validate(destBucket);
        S3ParamValidator.validate(sourceKey, "sourceKey");
        S3ParamValidator.validate(destKey, "destinationKey");

        try {
            CopyObjectRequest request = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(destBucket)
                    .destinationKey(destKey)
                    .build();
            s3.copyObject(request);
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "copy file", sourceBucket);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "copy file");
        }
    }

    @Override
    public void moveFile(String sourceBucket, String sourceKey, String destBucket, String destKey) throws StorageException {
        copyFile(sourceBucket, sourceKey, destBucket, destKey);
        deleteFile(sourceBucket, sourceKey);
    }

    @Override
    public Map<String, String> getFileInfo(String bucketName, String key) throws StorageException {
        runValidations(bucketName, Collections.singletonMap(key, "key"));

        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
            HeadObjectResponse response = s3.headObject(request);

            Map<String, String> info = new HashMap<>();
            info.put("Content-Length", String.valueOf(response.contentLength()));
            info.put("Last-Modified", response.lastModified().toString());
            info.put("Content-Type", response.contentType());
            info.put("ETag", response.eTag());
            return info;
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "get file info", bucketName);
            return null;
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "get file info");
            return null;
        }
    }

    @Override
    public List<String> listBuckets() throws StorageException {
        try {
            return s3.listBuckets().buckets().stream()
                    .map(Bucket::name)
                    .collect(Collectors.toList());
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "list buckets", null);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "list buckets");
        }
        return Collections.emptyList();
    }

    @Override
    public boolean bucketExists(String bucketName) throws StorageException {
        S3BucketValidator.validate(bucketName);

        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "bucket exists", bucketName);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "bucket exists");
        }
        return false;
    }

    @Override
    public void createBucket(String bucketName) throws StorageException {
        S3BucketValidator.validate(bucketName);

        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "create bucket", bucketName);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "create bucket");
        }
    }

    @Override
    public void deleteBucket(String bucketName) throws StorageException {
        S3BucketValidator.validate(bucketName);

        try {
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        } catch (S3Exception e) {
            S3ExceptionHandler.handle(e, "delete bucket", bucketName);
        } catch (Exception e) {
            S3ExceptionHandler.handleUnknownError(e, "delete bucket");
        }
    }
}