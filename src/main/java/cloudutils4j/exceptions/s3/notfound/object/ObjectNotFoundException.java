package cloudutils4j.exceptions.s3.notfound.object;

import cloudutils4j.exceptions.s3.io.StorageException;

public class ObjectNotFoundException extends StorageException {
    public ObjectNotFoundException(String message) {
        super(message);
    }
}