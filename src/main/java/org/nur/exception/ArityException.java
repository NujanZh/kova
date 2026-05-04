package org.nur.exception;

public class ArityException extends RuntimeException {
    public ArityException(String commandName) {
        super("wrong number of arguments for '" + commandName + "' command");
    }
}
