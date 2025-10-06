package com.cloudutils4j.s3;

import cloudutils4j.exceptions.s3.invalid.bucket.EmptyBucketNameException;
import cloudutils4j.exceptions.s3.invalid.bucket.InvalidBucketNameException;
import cloudutils4j.exceptions.s3.invalid.bucket.NullBucketNameException;
import cloudutils4j.exceptions.s3.invalid.key.*;
import cloudutils4j.exceptions.s3.invalid.localpath.EmptyDestinationLocalPathException;
import cloudutils4j.exceptions.s3.invalid.localpath.NullDestinationLocalPathException;
import cloudutils4j.exceptions.s3.invalid.localpath.NullLocalPathException;
import cloudutils4j.exceptions.s3.invalid.prefix.NullPrefixException;
import cloudutils4j.exceptions.s3.notfound.bucket.BucketDoesNotExistsException;
import cloudutils4j.exceptions.s3.io.StorageException;
import cloudutils4j.exceptions.s3.notfound.files.FileDoesNotExistsException;
import cloudutils4j.s3.impl.AwsSdkStorageOperations;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AwsSdkStorageOperationsTest {
    @Mock
    private S3Client mockS3Client;

    private AwsSdkStorageOperations storageOperations;

    @BeforeEach
    public void setUp() throws Exception {
        storageOperations = new AwsSdkStorageOperations(mockS3Client);
    }

    @Nested
    class ListFilesTest {
        @Test
        @DisplayName("[SUCCESS] listFiles should return a list of file keys")
        void listFiles_ShouldReturnListOfKeys_WhenBucketExists() throws Exception {
            String bucketName = "my-bucket";
            String prefix = "my-folder/";

            S3Object s3Object1 = S3Object.builder().key("my-folder/file1.txt").build();
            S3Object s3Object2 = S3Object.builder().key("my-folder/file2.png").build();

            ListObjectsV2Response fakeResponse = ListObjectsV2Response.builder()
                    .contents(Arrays.asList(s3Object1, s3Object2))
                    .build();

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(fakeResponse);

            List<String> result = storageOperations.listFiles(bucketName, prefix);

            assertNotNull(result);
            assertEquals(2, result.size());
            assertTrue(result.containsAll(Arrays.asList("my-folder/file1.txt", "my-folder/file2.png")));

            ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
            verify(mockS3Client, times(1)).listObjectsV2(requestCaptor.capture());
        }

        @Test
        @DisplayName("[SUCCESS] listFiles should return a list of file keys when prefix is empty string")
        void listFiles_ShouldReturnListOfKeys_WhenBucketExists_and_PrefixIsEmptyString() throws Exception {
            String bucketName = "my-bucket";
            String prefix = "";

            S3Object s3Object1 = S3Object.builder().key("my-folder/file1.txt").build();
            S3Object s3Object2 = S3Object.builder().key("my-folder/file2.png").build();
            S3Object s3Object3 = S3Object.builder().key("root_file.png").build();

            ListObjectsV2Response fakeResponse = ListObjectsV2Response.builder()
                    .contents(Arrays.asList(s3Object1, s3Object2, s3Object3))
                    .build();

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(fakeResponse);

            List<String> result = storageOperations.listFiles(bucketName, prefix);

            assertNotNull(result);
            assertEquals(3, result.size());
            assertTrue(result.containsAll(Arrays.asList("root_file.png", "my-folder/file1.txt", "my-folder/file2.png")));

            ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
            verify(mockS3Client, times(1)).listObjectsV2(requestCaptor.capture());
        }

        @Test
        @DisplayName("[SUCCESS] listFiles should return a list of file keys when prefix is not an argument")
        void listFiles_ShouldReturnListOfKeys_WhenBucketExists_and_PrefixIsNotPassed() throws Exception {
            String bucketName = "my-bucket";

            S3Object s3Object1 = S3Object.builder().key("my-folder/file1.txt").build();
            S3Object s3Object2 = S3Object.builder().key("my-folder/file2.png").build();
            S3Object s3Object3 = S3Object.builder().key("root_file.png").build();

            ListObjectsV2Response fakeResponse = ListObjectsV2Response.builder()
                    .contents(Arrays.asList(s3Object1, s3Object2, s3Object3))
                    .build();

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(fakeResponse);

            List<String> result = storageOperations.listFiles(bucketName);

            assertNotNull(result);
            assertEquals(3, result.size());
            assertTrue(result.containsAll(Arrays.asList("root_file.png", "my-folder/file1.txt", "my-folder/file2.png")));

            ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
            verify(mockS3Client, times(1)).listObjectsV2(requestCaptor.capture());
        }

        @Test
        @DisplayName("[SUCCESS] listFiles should return an empty list when bucket is empty")
        void listFiles_ShouldReturnEmptyList_WhenBucketIsEmpty() throws Exception {
            String bucketName = "my-bucket";

            ListObjectsV2Response fakeResponse = ListObjectsV2Response.builder()
                    .contents(Collections.emptyList())
                    .build();

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(fakeResponse);

            List<String> result = storageOperations.listFiles(bucketName);

            assertNotNull(result);
            assertEquals(0, result.size());
        }

        @Test
        @DisplayName("[FAILURE] listFiles should throw BucketDoesNotExistsException when bucket does not exist")
        void listFiles_ShouldThrowBucketDoesNotExistsException_WhenBucketDoesNotExist() throws Exception {
            String bucketName = "valid-bucket";
            String prefix = "prefix/";

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                    .thenThrow(NoSuchBucketException.builder().message("Bucket not found").build());

            assertThrows(BucketDoesNotExistsException.class, () -> {
                storageOperations.listFiles(bucketName, prefix);
            });

            verify(mockS3Client).listObjectsV2(any(ListObjectsV2Request.class));
        }

        @Test
        @DisplayName("[FAILURE] listFiles should throw InvalidBucketNameException")
        void listFiles_ShouldThrowException_WhenInvalidBucketName() throws Exception {
            String bucketName = "Invalid_BucketName";
            String prefix = "";

            assertThrows(InvalidBucketNameException.class, () -> {
                storageOperations.listFiles(bucketName, prefix);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("[FAILURE] listFiles should throw EmptyBucketNameException")
        void listFiles_ShouldThrowException_WhenEmptyBucketName() throws Exception {
            String bucketName = "";
            String prefix = "";

            assertThrows(EmptyBucketNameException.class, () -> {
               storageOperations.listFiles(bucketName, prefix);
            });

            verify(mockS3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        }

        @Test
        @DisplayName("[FAILURE] listFiles should throw NullBucketNameException")
        void listFiles_ShouldThrowException_WhenNullBucketName() throws Exception {
            String bucketName = null;
            String prefix = "";

            assertThrows(NullBucketNameException.class, () -> {
                storageOperations.listFiles(bucketName, prefix);
            });

            verify(mockS3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        }

        @Test
        @DisplayName("[FAILURE] listFiles should throw NullPrefixException")
        void listFiles_ShouldThrowException_WhenNullPrefix() throws Exception {
            String bucketName = "bucket";

            assertThrows(NullPrefixException.class, () -> {
                storageOperations.listFiles(bucketName, null);
            });

            verify(mockS3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        }

        @Test
        @DisplayName("[SUCCESS] listFiles should return empty list when prefix does not exist")
        void listFiles_ShouldReturnEmptyList_WhenPrefixDoesNotExist() throws Exception {
            String bucketName = "bucket";
            String prefix = "do-not-exist";

            ListObjectsV2Response fakeResponse = ListObjectsV2Response.builder()
                    .contents(Collections.emptyList())
                    .build();

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(fakeResponse);

            List<String> result = storageOperations.listFiles(bucketName, prefix);

            assertNotNull(result);
            assertEquals(0, result.size());
        }

        @Test
        @DisplayName("[FAILURE] listFiles should throw a generic error when unknown error")
        void listFiles_ShouldThrowStorageException_WhenUnknownError() throws Exception {
            String bucketName =  "bucket";
            String prefix = "prefix/";

            RuntimeException unknownError = new RuntimeException("Simulated network failure");

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                    .thenThrow(unknownError);

            StorageException thrownException = assertThrows(StorageException.class, () -> {
                storageOperations.listFiles(bucketName, prefix);
            });

            assertEquals(unknownError, thrownException.getCause());
        }
    }

    @Nested
    class fileExistsTest {
        @Test
        @DisplayName("[SUCCESS] Should return true if a file exists")
        public void fileExists_shouldReturnTrue_IfABucketExists() throws Exception {
            String bucketName = "bucket-exists";
            String key = "s3://existingbucket";

            when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                    .thenReturn(HeadObjectResponse.builder().build());

            boolean exists = storageOperations.fileExists(bucketName, key);

            assertTrue(exists);
        }

        @Test
        @DisplayName("[SUCCESS] fileExists should return false if a file does not exists")
        public void fileExists_ShouldReturnFalse_IfABucketDoesNotExist() throws Exception {
            String bucketName = "my-bucket";
            String key = "path/to/non-existent-file.txt";

            when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                    .thenThrow(NoSuchKeyException.class);

            boolean exists = storageOperations.fileExists(bucketName, key);

            assertFalse(exists);
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw BucketDoesNotExistsException when bucket does not exist")
        void fileExists_ShouldThrowBucketDoesNotExistsException_WhenBucketDoesNotExist() throws Exception {
            String bucketName = "non-existent-bucket";
            String key = "key-value";

            when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                    .thenThrow(NoSuchBucketException.builder().message("Bucket not found").build());

            assertThrows(BucketDoesNotExistsException.class, () -> {
                storageOperations.fileExists(bucketName, key);
            });
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw InvalidBucketNameException")
        public void fileExists_ShouldThrowException_WhenInvalidBucketName() throws Exception {
            String bucketName = "Invalid_BucketName";
            String key = "path/to/non-existent-file.txt";

            assertThrows(InvalidBucketNameException.class, () -> {
                storageOperations.fileExists(bucketName, key);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw EmptyBucketNameException")
        public void fileExists_ShouldThrowException_WhenEmptyBucketName() throws Exception {
            String bucketName = "";
            String key = "path/to/non-existent-file.txt";

            assertThrows(EmptyBucketNameException.class, () -> {
                storageOperations.fileExists(bucketName, key);
            });

            verify(mockS3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw NullBucketNameException")
        public void fileExists_ShouldThrowException_WhenNullBucketName() throws Exception {
            String key = "path/to/non-existent-file.txt";

            assertThrows(NullBucketNameException.class, () -> {
                storageOperations.fileExists(null, key);
            });

            verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw EmptyKeyException")
        public void fileExists_ShouldThrowException_WhenEmptyKey() throws Exception {
            String bucketName = "my-bucket";

            assertThrows(EmptyKeyException.class, () -> {
                storageOperations.fileExists(bucketName, "");
            });

            verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw NullKeyException")
        public void fileExists_ShouldThrowException_WhenNullKey() throws Exception {
            String bucketName = "my-bucket";

            assertThrows(NullKeyException.class, () -> {
                storageOperations.fileExists(bucketName, null);
            });

            verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
        }

        @Test
        @DisplayName("[FAILURE] fileExists should throw a generic error when unknown error")
        public void fileExists_ShouldThrowStorageException_WhenUnknownError() throws Exception {
            String bucketName = "my-bucket";
            String key = "path/to/non-existent-file.txt";

            RuntimeException unknownError = new RuntimeException("Simulated authentication failure");

            when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                    .thenThrow(unknownError);

            StorageException thrownException = assertThrows(StorageException.class, () -> {
                storageOperations.fileExists(bucketName, key);
            });

            assertEquals(unknownError, thrownException.getCause());
        }
    }

    @Nested
    class uploadFileTest {
        @Test
        @DisplayName("[SUCCESS] uploadFile should succesfully upload a file")
        public void uploadFile_ShouldSuccesfullyUploadFile(@TempDir Path tempDir) throws Exception {
            String bucketName = "my-bucket";
            String destinationKey = "destination/path/file.txt";

            Path tempFilePath = tempDir.resolve("test-upload.txt");
            Files.write(tempFilePath, "File content".getBytes());
            String localPath = tempFilePath.toAbsolutePath().toString();

            when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenReturn(PutObjectResponse.builder().build());

            storageOperations.uploadFile(localPath, bucketName, destinationKey);

            ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
            ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

            verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());
            PutObjectRequest capturedRequest = requestCaptor.getValue();

            assertEquals(bucketName, capturedRequest.bucket());
            assertEquals(destinationKey, capturedRequest.key());
            assertEquals(Files.size(tempFilePath), bodyCaptor.getValue().contentLength());
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw FileDoesNotExistsException")
        public void uploadFile_ShouldThrowFileDoesNotExistsException_WhenFileDoesNotExists() throws Exception {
            String localPath = "does-not/exist.txt";
            String bucketName = "my-bucket";
            String destinationKey = "path/to/non-existent-folder/";

            assertThrows(FileDoesNotExistsException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw BucketDoesNotExistsException when bucket does not exists")
        public void uploadFile_ShouldThrowBucketDoesNotExistsException_WhenBucketDoesNotExists(@TempDir Path tempDir) throws Exception {
            String bucketName = "dont-exist";
            String destinationKey = "files/subdirectory/upload.txt";

            Path tempFilePath = tempDir.resolve("test-upload.txt");
            Files.write(tempFilePath, "File content".getBytes());
            String localPath = tempFilePath.toAbsolutePath().toString();

            when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenThrow(NoSuchBucketException.builder().message("Bucket not found").build());

            assertThrows(BucketDoesNotExistsException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            verify(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw EmptyBucketNameException when bucket name is empty")
        public void uploadFile_ShouldThrowEmptyBucketNameException_WhenBucketNameIsEmpty() throws Exception {
            String localPath = "does-not/exist.txt";
            String bucketName = "";
            String destinationKey = "path/to/non-existent-folder/";

            assertThrows(EmptyBucketNameException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw EmptyLocalPathException when localPath is empty")
        public void uploadFile_ShouldThrowEmptyLocalPathException_WhenLocalPathIsEmpty() throws Exception {
            String localPath = "";
            String bucketName = "my-bucket";
            String destinationKey = "path/to/non-existent-folder/";

            assertThrows(EmptyDestinationLocalPathException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw EmptyDestinationKeyException when destinationKey is empty")
        public void uploadFile_ShouldThrowEmptyDestinationKeyException_WhenDestinationKeyIsEmpty() throws Exception {
            String localPath = "local-path/file.txt";
            String bucketName = "my-bucket";
            String destinationKey = "";

            assertThrows(EmptyDestinationKeyException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw NullBucketNameException when bucket name is null")
        public void uploadFile_ShouldThrowNullBucketNameException_WhenBucketNameIsNull() throws Exception {
            String localPath = "local-path/file.txt";
            String destinationKey = "path/to/non-existent-folder/";

            assertThrows(NullBucketNameException.class, () -> {
                storageOperations.uploadFile(localPath, null, destinationKey);
            });

            verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw NullLocalPathException when local path is null")
        public void uploadFile_ShouldThrowNullLocalPathException_WhenLocalPathIsNull() throws Exception {
            String bucketName = "my-bucket";
            String destinationKey = "path/to/non-existent-folder/";

            assertThrows(NullLocalPathException.class, () -> {
                storageOperations.uploadFile(null, bucketName, destinationKey);
            });

            verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw NullDestinationKeyException when destination key is null")
        public void uploadFile_ShouldThrowNullDestinationKeyException_WhenDestinationKeyIsNull() throws Exception {
            String localPath = "local-path/file.txt";
            String bucketName = "my-bucket";
            String destinationKey = null;

            assertThrows(NullDestinationKeyException.class, () -> {
               storageOperations.uploadFile(localPath, bucketName, null);
            });

            verify(mockS3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw InvalidBucketNameException when bucket name is invalid")
        public void uploadFile_ShouldThrowInvalidBucketNameException_WhenBucketNameIsInvalid() throws Exception {
            String localPath = "local-path/";
            String bucketName = "InvalidBucketName";
            String destinationKey = "path/to/non-existent-folder/";

            assertThrows(InvalidBucketNameException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("[FAILURE] uploadFile should throw a generic error when unknown error")
        public void uploadFile_ShouldThrowStorageException_WhenUnknownError(@TempDir File tempDir) throws Exception {
            String bucketName = "my-valid-bucket";
            String destinationKey = "destination/path/key.txt";

            Path tempFilePath = tempDir.toPath().resolve("upload.txt");
            Files.write(tempFilePath, "conte�do de teste".getBytes());
            String localPath = tempFilePath.toString();

            RuntimeException unknownError = new RuntimeException("Simulated network failure");
            when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenThrow(unknownError);

            StorageException thrownException = assertThrows(StorageException.class, () -> {
                storageOperations.uploadFile(localPath, bucketName, destinationKey);
            });

            assertEquals(unknownError, thrownException.getCause());
            verify(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
            }
        }

    @Nested
    class downloadFileTest {
        @Test
        @DisplayName("downloadFile should download file successfully when called")
        public void downloadFile_ShouldDownloadFileSuccessfully_WhenCalled(@TempDir File tempDir) throws Exception {
            String bucketName = "my-bucket";
            String sourceKey = "path/download.txt";
            Path localDestinationPath = tempDir.toPath().resolve("downloaded-file.txt");

            String expectedFileContent  = "This is a test file";

            doAnswer(invocation -> {
                ResponseTransformer<GetObjectResponse, Path> transformer = invocation.getArgument(1);

                GetObjectResponse response = GetObjectResponse.builder().contentLength((long) expectedFileContent.length()).build();
                transformer.transform(response, AbortableInputStream.create(new ByteArrayInputStream(expectedFileContent.getBytes())));

                return localDestinationPath;
            }).when(mockS3Client).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

            storageOperations.downloadFile(bucketName, sourceKey, localDestinationPath.toString());

            File downloadFile = localDestinationPath.toFile();
            assertTrue(downloadFile.exists(), "The downloaded file should have been created");

            String actualFileContent = new String(Files.readAllBytes(localDestinationPath));
            assertEquals(expectedFileContent, actualFileContent, "Unexpected downloaded content");

            ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
            verify(mockS3Client).getObject(requestCaptor.capture(), any(ResponseTransformer.class));

            GetObjectRequest request = requestCaptor.getValue();
            assertEquals(bucketName, request.bucket());
            assertEquals(sourceKey, request.key());
        }

        @Test
        @DisplayName("downloadFile should throw FileDoesNotExistsException when file does not exists")
        public void downloadFile_ShouldThrowFileDoesNotExistsException_WhenFileDoesNotExists() throws Exception {
            String bucketName = "my-bucket";
            String sourceKey = "path/download.txt";
            String localDestinationPath = "download-file.txt";

            when(mockS3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                    .thenThrow(NoSuchKeyException.class);

            assertThrows(FileDoesNotExistsException.class, () -> {
               storageOperations.downloadFile(bucketName, sourceKey, localDestinationPath);
            });

            verify(mockS3Client).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
        }

        @Test
        @DisplayName("downloadFile should throw BucketDoesNotExistsException when bucket does not exists")
        public void downloadFile_ShouldThrowBucketDoesNotExistsException_WhenBucketDoesNotExists() throws Exception {
            String bucketName = "my-bucket";
            String sourceKey = "path/download.txt";
            String localDestinationPath = "download-file.txt";

            when(mockS3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                    .thenThrow(NoSuchBucketException.class);

            assertThrows(BucketDoesNotExistsException.class, () -> {
               storageOperations.downloadFile(bucketName, sourceKey, localDestinationPath);
            });

            verify(mockS3Client).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
        }

        @Test
        @DisplayName("downloadFile should throw EmptyBucketNameException when bucket name is empty")
        public void downloadFile_ShouldThrowEmptyBucketNameException_WhenBucketNameIsEmpty() throws Exception {
            String sourceKey = "path/download.txt";
            String localDestinationPath = "download-file.txt";

            assertThrows(EmptyBucketNameException.class, () -> {
               storageOperations.downloadFile("", sourceKey, localDestinationPath);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw EmptySourceKeyException when source key is empty")
        public void downloadFile_ShouldThrowEmptySourceKeyException_WhenSourceKeyIsEmpty() throws Exception {
            String bucketName = "bucket-name";
            String sourceKey = "path/download.txt";
            String localDestinationPath = "download-file.txt";

            assertThrows(EmptySourceKeyException.class, () -> {
                storageOperations.downloadFile(bucketName, "", localDestinationPath);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw EmptyLocalPathException when local path is empty")
        public void downloadFile_ShouldThrowEmptyLocalPathException_WhenLocalPathIsEmpty() throws Exception {
            String bucketName = "bucket-name";
            String sourceKey = "path/download.txt";

            assertThrows(EmptyDestinationLocalPathException.class, () -> {
                storageOperations.downloadFile(bucketName, sourceKey, "");
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw NullBucketNameException when bucket name is null")
        public void downloadFile_ShouldThrowNullBucketNameException_WhenBucketNameIsNull() throws Exception {
            String sourceKey = "path/download.txt";
            String localDestinationPath = "download-file.txt";

            assertThrows(NullBucketNameException.class, () -> {
               storageOperations.downloadFile(null, sourceKey, localDestinationPath);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw NullLocalDestinationPathException when local destination path is null")
        public void downloadFile_ShouldThrowNullLocalDestinationPathException_WhenLocalDestinationPathIsNull() throws Exception {
            String bucketName = "bucket-name";
            String sourceKey = "path/download.txt";

            assertThrows(NullDestinationLocalPathException.class, () -> {
                storageOperations.downloadFile(bucketName, sourceKey, null);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw NullSourceKeyException when source key is null")
        public void downloadFile_ShouldThrowNullSourceKeyException_WhenSourceKeyIsNull() throws Exception {
            String bucketName = "bucket-name";
            String localDestinationPath = "download-file.txt";

            assertThrows(NullSourceKeyException.class, () -> {
               storageOperations.downloadFile(bucketName, null, localDestinationPath);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw InvalidBucketNameException when bucket name is invalid")
        public void downloadFile_ShouldThrowInvalidBucketNameException_WhenBucketNameIsInvalid() throws Exception {
            String bucketName = "Invalid_BucketName";
            String sourceKey = "path/download.txt";
            String localDestinationPath = "download-file.txt";

            assertThrows(InvalidBucketNameException.class, () -> {
               storageOperations.downloadFile(bucketName, sourceKey, localDestinationPath);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("downloadFile should throw Exception when unknown error")
        public void downloadFile_ShouldThrowStorageException_WhenUnknownError(@TempDir File tempDir) throws Exception {
            String bucketName = "my-bucket";
            String sourceKey = "s3/path/to/file.txt";
            Path localDestinationPath = tempDir.toPath().resolve("download-attempt.txt");

            RuntimeException unknownError = new RuntimeException("Simulated network failure");

            when(mockS3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                    .thenThrow(unknownError);
            StorageException thrownException = assertThrows(StorageException.class, () -> {
                storageOperations.downloadFile(bucketName, sourceKey, localDestinationPath.toString());
            });

            assertEquals(unknownError, thrownException.getCause());

            verify(mockS3Client).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
        }
    }

    @Nested
    class deleteFileTest {
        @Test
        @DisplayName("deleteFile should delete successfully when file exists")
        public void deleteFile_ShouldDeleteSuccessfully_WhenFileExists(@TempDir File tempDir) throws Exception {
            String bucketName = "my-bucket";
            String key = "s3/path/to/file.txt";

            storageOperations.deleteFile(bucketName, key);

            ArgumentCaptor<DeleteObjectRequest> captor = ArgumentCaptor.forClass(DeleteObjectRequest.class);

            verify(mockS3Client).deleteObject(captor.capture());

            DeleteObjectRequest capturedRequest = captor.getValue();

            assertAll(
                    () -> assertEquals(bucketName, capturedRequest.bucket(), "The bucket name provided on the request is incorrect."),
                    () -> assertEquals(key, capturedRequest.key(), "The key provided on the request is incorrect.")
            );
        }

        @Test
        @DisplayName("deleteFile should throw EmptyBucketNameException when bucket name is empty")
        public void deleteFile_ShouldThrowEmptyBucketNameException_WhenBucketNameIsEmpty() throws Exception {
            String key = "path/to/file.txt";

            assertThrows(EmptyBucketNameException.class, () -> {
               storageOperations.deleteFile("", key);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("deleteFile should throw EmptyKeyException when key is empty")
        public void deleteFile_ShouldThrowEmptyKeyException_WhenKeyIsEmpty() throws Exception {
            String bucketName = "bucket-name";
            String key = "";

            assertThrows(EmptyKeyException.class, () -> {
               storageOperations.deleteFile(bucketName, key);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("deleteFile should throw NullBucketNameException when bucket name is null")
        public void deleteFile_ShouldThrowNullBucketNameException_WhenBucketNameIsNull() throws Exception {
            String key = "path/to/file.txt";

            assertThrows(NullBucketNameException.class, () -> {
                storageOperations.deleteFile(null, key);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("deleteFile should throw NullKeyException when key is null")
        public void deleteFile_ShouldThrowNullKeyException_WhenKeyIsNull() throws Exception {
            String bucketName = "bucket-name";

            assertThrows(NullKeyException.class, () -> {
                storageOperations.deleteFile(bucketName, null);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("deleteFile should throw BucketDoesNotExistsException when bucket does not exists")
        public void deleteFile_ShouldThrowBucketDoesNotExistsException_WhenBucketDoesNotExists() throws Exception {
            String bucketName = "bucket-name";
            String key = "path/to/file.txt";

            when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
                    .thenThrow(NoSuchBucketException.builder().message("The specified bucket does not exist").build());

            assertThrows(BucketDoesNotExistsException.class, () -> {
                storageOperations.deleteFile(bucketName, key);
            });

            verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
        }

        @Test
        @DisplayName("deleteFile should throw InvalidBucketNameException when bucket name is invalid")
        public void deleteFile_ShouldThrowInvalidBucketNameException_WhenBucketNameIsInvalid() throws Exception {
            String bucketName = "Invalid_BucketName";
            String key = "s3/path/to/file.txt";

            assertThrows(InvalidBucketNameException.class, () -> {
               storageOperations.deleteFile(bucketName, key);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("deleteFile should throw StorageException when unknown error occurs")
        public void deleteFile_ShouldThrowStorageException_WhenUnknownErrorOccurs() throws Exception {
            String bucketName = "my-bucket";
            String key = "s3/path/to/file.txt";

            RuntimeException unknownError = new RuntimeException("Simulated network failure");

            when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
                    .thenThrow(unknownError);

            assertThrows(StorageException.class, () -> {
                storageOperations.deleteFile(bucketName, key);
            });

            verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
        }
    }

    @Nested
    class copyFileTest {
        @Test
        @DisplayName("copyFile should copy file successfully when file exists")
        public void copyFile_ShouldCopyFile_WhenFileExists() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/sub/";

            storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);

            ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);

            verify(mockS3Client).copyObject(captor.capture());

            CopyObjectRequest capturedRequest = captor.getValue();

            assertAll(
                    () -> assertEquals(sourceBucket, capturedRequest.sourceBucket(), "The source bucket provided on the request is incorrect."),
                    () -> assertEquals(sourceKey, capturedRequest.sourceKey(), "The destination bucket provided on the request is incorrect."),
                    () -> assertEquals(destBucket, capturedRequest.destinationBucket(), "The destination bucket provided on the request is incorrect."),
                    () -> assertEquals(destKey, capturedRequest.destinationKey(), "The destination bucket provided on the request is incorrect.")
            );
        }

        @Test
        @DisplayName("copyFile should throw FileDoesNotExistsException when file does not exists")
        public void copyFile_ShouldThrowFileDoesNotExistsException_WhenFileDoesNotExists() throws Exception {
            String  sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/does_not_exists.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/sub/";

            when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                    .thenThrow(NoSuchKeyException.builder().message("The specified sourceKey does not exist").build());

            assertThrows(FileDoesNotExistsException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
        }

        @Test
        @DisplayName("copyFile should throw BucketDoesNotExistsException when sourceBucket does not exists")
        public void copyFile_ShouldThrowBucketDoesNotExistsException_WhenSourceBucketDoesNotExists() throws Exception {
            String  sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/";

            when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                    .thenThrow(NoSuchBucketException.builder().message("The specified sourceBucket does not exist").build());

            assertThrows(BucketDoesNotExistsException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
        }

        @Test
        @DisplayName("copyFile should throw BucketDoesNotExistsException when destBucket does not exists")
        public void copyFile_ShouldThrowBucketDoesNotExistsException_WhenDestBucketDoesNotExists() throws Exception {
            String  sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/";

            when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                    .thenThrow(NoSuchBucketException.builder().message("The specified destBucket does not exist").build());

            assertThrows(BucketDoesNotExistsException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
        }

        @Test
        @DisplayName("copyFile should throw InvalidBucketNameException when sourceBucket name is invalid")
        public void copyFile_ShouldThrowInvalidBucketNameException_WhenSourceBucketNameIsInvalid() throws Exception {
            String  sourceBucket = "Invalid_BucketName";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/";

            assertThrows(InvalidBucketNameException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw InvalidBucketNameException when destBucket name is invalid")
        public void copyFile_ShouldThrowInvalidBucketNameException_WhenDestBucketNameIsInvalid() throws Exception {
            String  sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "Invalid_BucketName";
            String destKey = "s3/path/to/";

            assertThrows(InvalidBucketNameException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw EmptyBucketNameException when sourceBucket name is empty")
        public void copyFile_ShouldThrowEmptyBucketNameException_WhenSourceBucketNameIsEmpty() throws Exception {
            String  sourceBucket = "";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "Invalid_BucketName";
            String destKey = "s3/path/to/";

            assertThrows(EmptyBucketNameException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw EmptyBucketNameException when destBucket name is empty")
        public void copyFile_ShouldThrowEmptyBucketNameException_WhenDestBucketNameIsEmpty() throws Exception {
            String  sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "";
            String destKey = "s3/path/to/sub/";

            assertThrows(EmptyBucketNameException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw NullBucketNameException when sourceBucket is null")
        public void copyFile_ShouldThrowNullBucketNameException_WhenSourceBucketIsNull() throws Exception {
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/sub/";

            assertThrows(NullBucketNameException.class, () -> {
                storageOperations.copyFile(null, sourceKey, destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw NullBucketNameException when destBucket is null")
        public void copyFile_ShouldThrowNullBucketNameException_WhenDestBucketIsNull() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destKey = "s3/path/to/sub/";

            assertThrows(NullBucketNameException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, null, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw EmptySourceKeyException when sourceKey is empty")
        public void copyFile_ShouldThrowEmptySourceKeyException_WhenSourceKeyIsEmpty() throws Exception {
            String  sourceBucket = "source-bucket";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/sub/";

            assertThrows(EmptySourceKeyException.class, () -> {
                storageOperations.copyFile(sourceBucket, "", destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw EmptyDestKeyException when destKey is empty")
        public void copyFile_ShouldThrowEmptyDestinationKeyException_WhenDestKeyIsEmpty() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";

            assertThrows(EmptyDestinationKeyException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, "");
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw NullSourceKeyException when sourceKey is null")
        public void copyFile_ShouldThrowNullSourceKeyException_WhenSourceKeyIsNull() throws Exception {
            String sourceBucket = "source-bucket";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/";

            assertThrows(NullSourceKeyException.class, () -> {
                storageOperations.copyFile(sourceBucket, null, destBucket, destKey);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw NullDestinationKeyException when sourceKey is null")
        public void copyFile_ShouldThrowNullDestinationKeyException_WhenSourceKeyIsNull() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";

            assertThrows(NullDestinationKeyException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, null);
            });

            verifyNoInteractions(mockS3Client);
        }

        @Test
        @DisplayName("copyFile should throw StorageException when unknown error occurs")
        public void copyFile_ShouldThrowStorageException_WhenUnknownErrorOccurs() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "s3/path/to/file.txt";
            String destBucket = "dest-bucket";
            String destKey = "s3/path/to/sub/";

            RuntimeException unknownError = new RuntimeException("Simulated network failure");

            when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                    .thenThrow(unknownError);

            assertThrows(StorageException.class, () -> {
                storageOperations.copyFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
        }
    }

    @Nested
    class moveFileTest {
        @Test
        @DisplayName("should call copy and then delete on successful move")
        public void moveFile_ShouldCallCopyThenDelete_WhenSuccessful() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "path/source.txt";
            String destBucket = "dest-bucket";
            String destKey = "path/dest.txt";

            storageOperations.moveFile(sourceBucket, sourceKey, destBucket, destKey);

            InOrder inOrder = inOrder(mockS3Client);
            ArgumentCaptor<CopyObjectRequest> copyCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
            ArgumentCaptor<DeleteObjectRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);

            inOrder.verify(mockS3Client).copyObject(copyCaptor.capture());
            inOrder.verify(mockS3Client).deleteObject(deleteCaptor.capture());

            CopyObjectRequest capturedCopyRequest = copyCaptor.getValue();
            assertEquals(sourceBucket, capturedCopyRequest.sourceBucket());
            assertEquals(sourceKey, capturedCopyRequest.sourceKey());
            assertEquals(destBucket, capturedCopyRequest.destinationBucket());
            assertEquals(destKey, capturedCopyRequest.destinationKey());

            DeleteObjectRequest capturedDeleteRequest = deleteCaptor.getValue();
            assertEquals(sourceBucket, capturedDeleteRequest.bucket());
            assertEquals(sourceKey, capturedDeleteRequest.key());
        }

        @Test
        @DisplayName("should not call delete if the copy operation fails")
        public void moveFile_ShouldNotCallDelete_WhenCopyFails() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "non-existent.txt";
            String destBucket = "dest-bucket";
            String destKey = "path/dest.txt";

            when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                    .thenThrow(NoSuchKeyException.builder().message("The specified key does not exist").build());

            assertThrows(FileDoesNotExistsException.class, () -> {
                storageOperations.moveFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verify(mockS3Client, never()).deleteObject(any(DeleteObjectRequest.class));
        }

        @Test
        @DisplayName("should propagate exception when delete fails after a successful copy")
        public void moveFile_ShouldPropagateException_WhenDeleteFails() throws Exception {
            String sourceBucket = "source-bucket";
            String sourceKey = "source.txt";
            String destBucket = "dest-bucket";
            String destKey = "path/dest.txt";

            when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
                    .thenThrow(S3Exception.builder().message("Access Denied").build());

            assertThrows(StorageException.class, () -> {
                storageOperations.moveFile(sourceBucket, sourceKey, destBucket, destKey);
            });

            verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
            verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
        }
    }
}
