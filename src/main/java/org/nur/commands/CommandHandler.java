package org.nur.commands;

import org.nur.persistence.AofWriter;
import org.nur.protocol.RespValue;
import org.nur.storage.StorageEngine;

import java.io.IOException;
import java.util.List;

public class CommandHandler {

    private final StorageEngine storageEngine;
    private final AofWriter aofWriter;

    public CommandHandler(StorageEngine storageEngine, AofWriter aofWriter) {
        this.storageEngine = storageEngine;
        this.aofWriter = aofWriter;
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
                case "EXPIREAT" -> handleExpireAt(elements);
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
        appendAof(command);

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

        boolean deleted = storageEngine.delete(key);

        if (deleted) {
            appendAof(command);
            return new RespValue.Integer(1);
        }

        return new RespValue.Integer(0);
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
            long absoluteTtl = System.currentTimeMillis() + ttl * 1000;

            boolean updated = storageEngine.expire(key, ttl);

            if (updated) {
                List<RespValue> aofCommand =
                        List.of(
                                new RespValue.BulkString("EXPIRE"),
                                new RespValue.BulkString(key),
                                new RespValue.BulkString(String.valueOf(absoluteTtl)));
                appendAof(aofCommand);
            }

            return new RespValue.Integer(updated ? 1 : 0);
        } catch (NumberFormatException e) {
            return RespValue.Error.err("value is not an integer or out of range");
        }
    }

    private RespValue handleExpireAt(List<RespValue> command) {
        String key = extractString(command, 1);
        String epochStr = extractString(command, 2);

        if (key == null || epochStr == null) {
            return RespValue.Error.err("wrong number of arguments for 'EXPIRE' command");
        }

        try {
            long epoch = Long.parseLong(epochStr);
            return new RespValue.Integer(storageEngine.expireAt(key, epoch) ? 1 : 0);
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

    private void appendAof(List<RespValue> command) {
        try {
            RespValue.Array aofInput = new RespValue.Array(command);
            aofWriter.append(aofInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
