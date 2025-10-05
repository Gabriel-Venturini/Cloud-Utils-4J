package cloudutils4j.s3.utils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

/**
 * Factory class responsible for building and configuring the AWS S3Client.
 * This centralizes the client initialization logic, adhering to the Single Responsibility Principle (SRP).
 *
 * @author Gabriel Venturini
 */
public final class S3ClientBuilderFactory {

    private S3ClientBuilderFactory() {}

    /**
     * Builds and returns a fully configured S3Client using provided connection details.
     * This method handles credential creation and delegates the client construction.
     *
     * @param endpoint  The service endpoint (e.g., "http://localhost:9000").
     * @param region    The service region (e.g., "us-east-1").
     * @param accessKey The access key for authentication.
     * @param secretKey The secret key for authentication.
     * @return A ready-to-use S3Client instance.
     */
    public static S3Client buildClient(String endpoint, String region, String accessKey, String secretKey) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        return createClient(endpoint, region, credentials);
    }

    /**
     * Private helper method to construct the S3Client using the provided credentials.
     *
     * @param endpoint The service endpoint URI.
     * @param region The AWS Region.
     * @param credentials The AWS Basic Credentials.
     * @return The built S3Client instance.
     */
    private static S3Client createClient(String endpoint, String region, AwsBasicCredentials credentials) {
        return S3Client.builder()
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }
}
