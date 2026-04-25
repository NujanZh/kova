package org.nur.exception;

public class ClientDisconnectedException extends RuntimeException {
    public ClientDisconnectedException() {
        super("Client disconnected");
    }
}
