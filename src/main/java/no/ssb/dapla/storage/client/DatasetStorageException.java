package no.ssb.dapla.storage.client;

public class DatasetStorageException extends RuntimeException {

    public DatasetStorageException(Throwable cause) {
        super(cause);
    }

    public DatasetStorageException(String message, Throwable cause) {
        super(message, cause);
    }

}

