package org.nur.exception;

public class AofQueueFullException extends RuntimeException {
    public AofQueueFullException(long timeoutMs) {
        super("AOF queue is full after " + timeoutMs + "ms - disk I/O can't keep up");
    }
}
