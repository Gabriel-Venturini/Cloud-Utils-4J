# CloudUtils4J - Abstraction Layer for Cloud Storage (Alpha)

<p>CloudUtils4J is a lightweight Java utility library (Java 8 compatible) designed to simplify, standardize, and abstract cloud storage operations, initially focusing on AWS S3 and S3-compatible services like MinIO.</p>

## Architectural Value Proposition

This project addresses key architectural concerns common in enterprise integration, moving from raw SDK usage to a controlled, predictable utility layer.

1. **Anti-Corruption Layer (ACL)**: The library acts as a protective buffer, shielding core application business logic from the complexities and external dependencies of the AWS SDK.
2. **Vendor Decoupling (Facade Pattern)**: The design is driven by the generic ```StorageOperations``` interface. This **Facade Pattern** guarantees that the consuming application is fully decoupled from the underlying vendor implementation (```AwsSdkStorageOperations```), facilitating future cloud migration (e.g., Azure or GCP) without modifying business code.
3. **Standardized Error Handling**: It transforms the numerous, verbose, and provider-specific exceptions (e.g., ```NoSuchKeyException```, generic ```S3Exception``` codes) into a clean, predictable, and manageable custom hierarchy based on ```StorageException```. This simplifies application-level error catching and resolution.
4. **Enhanced Testability**: The clear interface-implementation separation ensures that the ```StorageOperations``` layer can be easily mocked in unit tests, allowing for rigorous validation of business logic without dependence on network calls or external systems.

## Getting Started (Maven Integration)

To include the library in your Java 8 project:
```xml
<dependency>
    <groupId>org.cloudutils4j</groupId>
    <artifactId>CloudUtils4J</artifactId>
    <version>1.0.3-Stable</version>
</dependency>
```

### Key Technologies

- Language: Java 8
- Build Tool: Apache Maven
- Cloud Provider: AWS SDK v2 (software.amazon.awssdk:s3)
- Testing: JUnit 5 and Mockito

### Quick Usage Example

The library standardizes and wraps common storage tasks:

```java
// Instantiates the abstraction layer, often managed by Spring or CDI
StorageOperations storage = new AwsSdkStorageOperations(endpoint, region, accessKey, secretKey);

try {
// Standardized Operations: Single, clear method call
storage.uploadFile(LOCAL_PATH, BUCKET_NAME, UPLOAD_KEY);

    // Complex operation wrapped into one method (Copy + Delete Source)
    storage.moveFile(SOURCE_BUCKET, SOURCE_KEY, DEST_BUCKET, DEST_KEY); 

} catch (StorageException e) {
// Predictable and centralized error handling using custom exceptions
logger.error("A standardized storage error occurred: {}", e.getMessage());
}
```

### Project Structure

| Directory                                | Purpose                                                                                       |
|------------------------------------------|-----------------------------------------------------------------------------------------------|
| ```src/main/java/com/cloudutils4j/core```       | Core abstraction interfaces (```StorageOperations```).                                        |
| ```src/main/java/com/cloudutils4j/s3/impl```    | AWS SDK v2 implementation (the vendor wrapper).                                               |
| ```src/main/java/com/cloudutils4j/exceptions``` | Custom exception hierarchy (ACL implementation).                                              |
| ```src/test/java```                             | Dedicated space for **Unit Tests** (JUnit 5, Mockito).                                        |
| ```src/demo/java```                             | Isolated code for demonstration and integration testing, excluded from the final library JAR. |
