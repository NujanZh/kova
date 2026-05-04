package org.nur.exception;

public class AofException extends ServerException {
    public AofException(String message) {
        super(message);
    }

    public AofException(String message, Throwable cause) {
        super(message, cause);
    }
}
