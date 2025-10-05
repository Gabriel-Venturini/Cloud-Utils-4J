package com.cloudutils4j.demo;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvException;
import org.cloudutils4j.s3.impl.AwsSdkStorageOperations;
import org.cloudutils4j.core.StorageOperations;
import org.cloudutils4j.exceptions.StorageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Demonstration and Integration Test Class for the CloudUtils4J library.
 *
 * This class simulates a consumer project using the library for S3 operations.
 * It requires a configured .env file in the project root to load credentials and paths.
 *
 */
public class DemoMain {
    private static final Logger logger = LogManager.getLogger(DemoMain.class);

    // Environment variables configured in the .env for the test.
    private static final String BUCKET_NAME;
    private static final String LOCAL_FILE_PATH;
    private static final String DOWNLOAD_PATH;
    private static final String UPLOAD_KEY;
    private static final String COPY_KEY;
    private static final String MOVE_KEY;

    // Static block to load the .env and initialize variables
    static {
        try {
            // Uses dotenv-java, which is in the 'runtime' scope of the pom.xml
            Dotenv dotenv = Dotenv.load();

            BUCKET_NAME = dotenv.get("BUCKET_NAME");
            LOCAL_FILE_PATH = dotenv.get("LOCAL_FILE_PATH");
            DOWNLOAD_PATH = dotenv.get("DOWNLOAD_PATH");
            UPLOAD_KEY = dotenv.get("UPLOAD_KEY");
            COPY_KEY = dotenv.get("COPY_KEY");
            MOVE_KEY = dotenv.get("MOVE_KEY");

            if (BUCKET_NAME == null || LOCAL_FILE_PATH == null || DOWNLOAD_PATH == null
                    || UPLOAD_KEY == null || COPY_KEY == null || MOVE_KEY == null) {
                // If a mandatory variable is missing, throw exception and stop.
                throw new IllegalStateException("One or more mandatory environment variables are not configured in the .env");
            }
        } catch (DotenvException e) {
            logger.error("Failed to load environment variables. Check the .env file.", e);
            throw new RuntimeException("Could not load environment variables", e);
        }
    }

    public static void main(String[] args) {
        // Credentials for MinIO/S3 (if MinIO, use localhost)
        // In a real production environment, credentials would come from a Secrets Manager or IAM/Instance Profile.
        String endpoint = "http://localhost:9000"; // Example for MinIO
        String region = "us-east-1";
        String accessKey = "admin";
        String secretKey = "admin123";

        // Instantiates the operation, using its abstraction interface
        StorageOperations storage = new AwsSdkStorageOperations(endpoint, region, accessKey, secretKey);

        // 1. Creates the local test file
        createLocalTestFile();

        try {
            logger.info("--- STARTING S3/MINIO OPERATION TESTS ---");

            // 2. Creates the Bucket
            logger.info("[1/8] Checking and creating bucket '{}'...", BUCKET_NAME);
            if (!storage.bucketExists(BUCKET_NAME)) {
                storage.createBucket(BUCKET_NAME);
                logger.info(">>> Bucket '{}' successfully created.", BUCKET_NAME);
            } else {
                logger.warn(">>> Bucket '{}' already existed. Proceeding.", BUCKET_NAME);
            }

            // 3. File Upload
            logger.info("[2/8] Uploading file '{}' to '{}'...", LOCAL_FILE_PATH, UPLOAD_KEY);
            storage.uploadFile(LOCAL_FILE_PATH, BUCKET_NAME, UPLOAD_KEY);
            logger.info(">>> Upload complete.");
            if (!storage.fileExists(BUCKET_NAME, UPLOAD_KEY)) {
                throw new IllegalStateException("ERROR: The uploaded file was not found!");
            }
            logger.info(">>> Existence check (fileExists) after upload: OK");

            // 4. Listing and Info
            logger.info("[3/8] Listing files and getting information...");
            List<String> files = storage.listFiles(BUCKET_NAME, "tests/");
            logger.info(">>> Files found in prefix 'tests/': " + files);
            Map<String, String> info = storage.getFileInfo(BUCKET_NAME, UPLOAD_KEY);
            logger.info(">>> File info (getFileInfo): Size = {} bytes", info.get("Content-Length"));

            // 5. Copying
            logger.info("[4/8] Copying file to '{}'...", COPY_KEY);
            storage.copyFile(BUCKET_NAME, UPLOAD_KEY, BUCKET_NAME, COPY_KEY);
            logger.info(">>> Copy complete.");
            if (!storage.fileExists(BUCKET_NAME, COPY_KEY)) {
                throw new IllegalStateException("ERROR: The copied file was not found!");
            }
            logger.info(">>> Existence check (fileExists) after copy: OK");

            // 6. Moving (Copy + Delete Source)
            logger.info("[5/8] Moving copied file to '{}'...", MOVE_KEY);
            storage.moveFile(BUCKET_NAME, COPY_KEY, BUCKET_NAME, MOVE_KEY);
            logger.info(">>> Move complete.");
            if (storage.fileExists(BUCKET_NAME, COPY_KEY)) {
                throw new IllegalStateException("ERROR: The source file for the move still exists!");
            }
            if (!storage.fileExists(BUCKET_NAME, MOVE_KEY)) {
                throw new IllegalStateException("ERROR: The moved file was not found at the destination!");
            }
            logger.info(">>> Move Verification (source deleted, destination exists): OK");

            // 7. Download
            logger.info("[6/8] Downloading original file to '{}'...", DOWNLOAD_PATH);
            storage.downloadFile(BUCKET_NAME, UPLOAD_KEY, DOWNLOAD_PATH);
            logger.info(">>> Download complete.");
            if (!new File(DOWNLOAD_PATH).exists()) {
                throw new IllegalStateException("ERROR: The downloaded file was not created locally!");
            }
            logger.info(">>> Existence check of the downloaded file: OK");

            // 8. Object Deletion
            logger.info("[7/8] Deleting objects '{}' and '{}' from the bucket...", UPLOAD_KEY, MOVE_KEY);
            storage.deleteFile(BUCKET_NAME, UPLOAD_KEY);
            storage.deleteFile(BUCKET_NAME, MOVE_KEY);
            logger.info(">>> Objects deleted.");
            if (storage.fileExists(BUCKET_NAME, UPLOAD_KEY) || storage.fileExists(BUCKET_NAME, MOVE_KEY)) {
                throw new IllegalStateException("ERROR: One of the deleted files still exists in the bucket!");
            }
            logger.info(">>> Deletion Check (deleteFile): OK");

            // 9. Bucket Deletion
            logger.info("[8/8] Deleting test bucket '{}'...", BUCKET_NAME);
            // Before deleting, ensure any residual files are removed (best practice)
            storage.listFiles(BUCKET_NAME, "").forEach(key -> {
                try { storage.deleteFile(BUCKET_NAME, key); } catch (StorageException ignored) {}
            });
            storage.deleteBucket(BUCKET_NAME);
            logger.info(">>> Bucket deleted.");
            if (storage.bucketExists(BUCKET_NAME)) {
                throw new IllegalStateException("ERROR: The test bucket was not deleted!");
            }
            logger.info(">>> Deletion Check (deleteBucket): OK");

            logger.info("--- SUCCESS: ALL OPERATIONS EXECUTED CORRECTLY ---");

        } catch (StorageException e) {
            // This catch demonstrates the standardized error handling of your library!
            logger.error("!!! EXPECTED AND STANDARDIZED LIBRARY ERROR !!!", e);
        } catch (Exception e) {
            logger.error("!!! AN UNEXPECTED ERROR OCCURRED DURING THE TEST !!!", e);
        } finally {
            // Final cleanup block to ensure local and remote resources are removed
            logger.info("--- STARTING FINAL CLEANUP (FINALLY BLOCK) ---");

            // Cleaning up local files
            deleteLocalTestFile(LOCAL_FILE_PATH);
            deleteLocalTestFile(DOWNLOAD_PATH);
            logger.info("Local test files removed.");
        }
    }

    /** Helper method to create a dummy file for testing. */
    private static void createLocalTestFile() {
        try (PrintWriter out = new PrintWriter(LOCAL_FILE_PATH)) {
            out.println("This is a test file for the CloudUtils4J operations.");
            out.println("Timestamp: " + System.currentTimeMillis());
            logger.info("Local test file created at: {}", LOCAL_FILE_PATH);
        } catch (IOException e) {
            logger.error("Failed to create local test file", e);
            throw new RuntimeException(e);
        }
    }

    /** Helper method to clean up local files. */
    private static void deleteLocalTestFile(String path) {
        try {
            Files.deleteIfExists(Paths.get(path));
        } catch (IOException e) {
            logger.warn("Could not delete local file: {}", path);
        }
    }
}
