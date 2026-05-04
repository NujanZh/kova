package org.nur.commands;

import org.nur.exception.ArityException;
import org.nur.persistence.AofWriter;
import org.nur.protocol.RespValue;
import org.nur.storage.StorageEngine;

import java.util.List;

public class CommandHandler {

    private final StorageEngine storageEngine;
    private final AofWriter aofWriter;

    public CommandHandler(StorageEngine storageEngine, AofWriter aofWriter) {
        this.storageEngine = storageEngine;
        this.aofWriter = aofWriter;
    }

    public CommandHandler(StorageEngine storageEngine) {
        this(storageEngine, null);
    }

    public RespValue handle(RespValue input) {
        if (!(input instanceof RespValue.Array(List<RespValue> elements)) || elements.isEmpty()) {
            return RespValue.Error.err("expected array command");
        }

        Command command = Command.parse(elements);

        if (command == null) {
            return RespValue.Error.err("wrong number of arguments for command");
        }

        try {
            return switch (command.name()) {
                case "PING" -> new RespValue.SimpleString("PONG");
                case "SET" -> handleSet(command);
                case "GET" -> handleGet(command);
                case "DEL" -> handleDel(command);
                case "EXISTS" -> handleExists(command);
                case "EXPIRE" -> handleExpire(command);
                case "EXPIREAT" -> handleExpireAt(command);
                case "TTL" -> handleTtl(command);
                default -> RespValue.Error.err("unknown command '" + command.name() + "'");
            };
        } catch (ArityException e) {
            return RespValue.Error.err(e.getMessage());
        }
    }

    private RespValue handleSet(Command command) {
        String key = command.requeireString(0);
        String value = command.requeireString(1);

        storageEngine.set(key, value);
        appendAof(command.args());
        return new RespValue.SimpleString("OK");
    }

    private RespValue handleGet(Command command) {
        String key = command.requeireString(0);
        return new RespValue.BulkString(storageEngine.get(key));
    }

    private RespValue handleDel(Command command) {
        String key = command.requeireString(0);

        boolean deleted = storageEngine.delete(key);
        if (deleted) appendAof(command.args());

        return new RespValue.Integer(deleted ? 1 : 0);
    }

    private RespValue handleExists(Command command) {
        String key = command.requeireString(0);
        return new RespValue.Integer(storageEngine.exists(key) ? 1 : 0);
    }

    private RespValue handleExpire(Command command) {
        String key = command.requeireString(0);
        String seconds = command.requeireString(1);

        try {
            long ttl = Long.parseLong(seconds);
            long absoluteTtl = System.currentTimeMillis() + ttl * 1000;

            boolean updated = storageEngine.expire(key, absoluteTtl);

            if (updated) {
                List<RespValue> aofCommand =
                        List.of(
                                new RespValue.BulkString("EXPIREAT"),
                                new RespValue.BulkString(key),
                                new RespValue.BulkString(String.valueOf(absoluteTtl)));
                appendAof(aofCommand);
            }

            return new RespValue.Integer(updated ? 1 : 0);
        } catch (NumberFormatException e) {
            return RespValue.Error.err("value is not an integer or out of range");
        }
    }

    private RespValue handleExpireAt(Command command) {
        String key = command.requeireString(0);
        String epochStr = command.requeireString(1);

        try {
            long epochSeconds = Long.parseLong(epochStr);
            long epoch = epochSeconds * 1000;
            return new RespValue.Integer(storageEngine.expire(key, epoch) ? 1 : 0);
        } catch (NumberFormatException e) {
            return RespValue.Error.err("value is not an integer or out of range");
        }
    }

    private RespValue handleTtl(Command command) {
        String key = command.requeireString(0);
        return new RespValue.Integer(storageEngine.getTtl(key));
    }

    private void appendAof(List<RespValue> command) {
        if (aofWriter == null) return;
        RespValue.Array aofInput = new RespValue.Array(command);
        aofWriter.append(aofInput);
    }
}
