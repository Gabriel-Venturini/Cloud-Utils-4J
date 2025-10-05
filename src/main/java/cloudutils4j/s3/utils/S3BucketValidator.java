package cloudutils4j.s3.utils;

import cloudutils4j.exceptions.s3.invalid.bucket.EmptyBucketNameException;
import cloudutils4j.exceptions.s3.invalid.bucket.InvalidBucketNameException;
import cloudutils4j.exceptions.s3.invalid.bucket.NullBucketNameException;

import java.util.regex.Pattern;

/**
 * **Utility class for validating Amazon Simple Storage Service (S3) bucket names**
 * against the strict naming conventions enforced by AWS.
 * <p>
 * This class ensures that a given bucket name is not null, not empty, and
 * conforms to all AWS S3 rules, including length, allowed characters, and format.
 * </p>
 * <p>
 * **Validation Rules include:**
 * <ul>
 * <li>Must be between 3 and 63 characters long.</li>
 * <li>Can consist only of lowercase letters, numbers, dots (.), and hyphens (-).</li>
 * <li>Must begin and end with a letter or number.</li>
 * <li>Cannot be formatted as an IP address (e.g., 192.168.5.4).</li>
 * </ul>
 * </p>
 *
 * @author Gabriel Venturini
 */
public final class S3BucketValidator {

    private static final Pattern BUCKET_PATTERN =
        Pattern.compile("^(?=.{3,63}$)(?!(\\d+\\.){3}\\d+$)([a-z0-9](-?[a-z0-9])*(\\.[a-z0-9](-?[a-z0-9])*)*)$");

    private S3BucketValidator() {}

    public static void validate(String bucketName) {
        if (bucketName == null) throw new NullBucketNameException("Bucket name cannot be null!");
        if (bucketName.isEmpty()) throw new EmptyBucketNameException("Bucket name cannot be empty!");
        if (!BUCKET_PATTERN.matcher(bucketName).matches()) {
            throw new InvalidBucketNameException("Invalid bucket name: " + bucketName +
                    ". It must follow AWS naming rules.");
        }
    }
}
