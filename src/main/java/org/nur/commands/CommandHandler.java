package org.nur.commands;

import org.nur.protocol.RespValue;
import org.nur.storage.StorageEngine;

import java.util.List;

public class CommandHandler {

    private final StorageEngine storageEngine;

    public CommandHandler(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    public RespValue handle(RespValue command) {
        if (!(command instanceof RespValue.Array(List<RespValue> elements))) {
            return RespValue.Error.err("expected array command");
        }

        if (elements.isEmpty()) {
            return RespValue.Error.err("empty command");
        }

        if ((elements.getFirst() instanceof RespValue.BulkString(String operation))) {
            return switch (operation.toUpperCase()) {
                case "PING" -> new RespValue.SimpleString("PONG");
                case "SET" -> handleSet(elements);
                case "GET" -> handleGet(elements);
                case "DEL" -> handleDel(elements);
                case "EXISTS" -> handleExists(elements);
                case "EXPIRE" -> handleExpire(elements);
                case "TTL" -> handleTtl(elements);
                default -> RespValue.Error.err("unknown command '" + operation + "'");
            };
        } else {
            return RespValue.Error.err("wrong number of arguments for command");
        }
    }

    private RespValue handleSet(List<RespValue> command) {
        String key = extractString(command, 1);
        String value = extractString(command, 2);

        if (key == null || value == null) {
            return RespValue.Error.err("wrong number of arguments for 'SET' command");
        }

        storageEngine.set(key, value);

        return new RespValue.SimpleString("OK");
    }

    private RespValue handleGet(List<RespValue> command) {
        String key = extractString(command, 1);

        if (key == null) {
            return RespValue.Error.err("wrong number of arguments for 'GET' command");
        }

        return new RespValue.BulkString(storageEngine.get(key));
    }

    private RespValue handleDel(List<RespValue> command) {
        String key = extractString(command, 1);

        if (key == null) {
            return RespValue.Error.err("wrong number of arguments for 'DEL' command");
        }

        return new RespValue.Integer(storageEngine.delete(key) ? 1 : 0);
    }

    private RespValue handleExists(List<RespValue> command) {
        String key = extractString(command, 1);

        if (key == null) {
            return RespValue.Error.err("wrong number of arguments for 'EXISTS' command");
        }

        return new RespValue.Integer(storageEngine.exists(key) ? 1 : 0);
    }

    private RespValue handleExpire(List<RespValue> command) {
        String key = extractString(command, 1);
        String seconds = extractString(command, 2);

        if (key == null || seconds == null) {
            return RespValue.Error.err("wrong number of arguments for 'EXPIRE' command");
        }

        try {
            long ttl = Long.parseLong(seconds);
            return new RespValue.Integer(storageEngine.expire(key, ttl) ? 1 : 0);
        } catch (NumberFormatException e) {
            return RespValue.Error.err("value is not an integer or out of range");
        }
    }

    private RespValue handleTtl(List<RespValue> command) {
        String key = extractString(command, 1);
        if (key == null) {
            return RespValue.Error.err("wrong number of arguments for 'TTL' command");
        }

        return new RespValue.Integer(storageEngine.getTtl(key));
    }

    private String extractString(List<RespValue> command, int index) {
        if (command.size() <= index) {
            return null;
        }
        if (!(command.get(index) instanceof RespValue.BulkString(String value))) {
            return null;
        }
        return value;
    }
}
