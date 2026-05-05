package org.nur.commands;

import org.nur.exception.AofQueueFullException;
import org.nur.exception.ArityException;
import org.nur.persistence.AofWriter;
import org.nur.protocol.RespValue;
import org.nur.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CommandHandler {

    private static final Logger log = LoggerFactory.getLogger(CommandHandler.class);

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
            log.warn("Received malformed command - expected non-empty array, got: {}", input);
            return RespValue.Error.err("expected array command");
        }

        Command command = Command.parse(elements);
        if (command == null) {
            log.warn("Received command with invalid format: {}", elements);
            return RespValue.Error.err("wrong number of arguments for command");
        }

        log.debug("Handling command: {}", command.name());

        try {
            RespValue result =
                    switch (command.name()) {
                        case "PING" -> new RespValue.SimpleString("PONG");
                        case "SET" -> handleSet(command);
                        case "GET" -> handleGet(command);
                        case "DEL" -> handleDel(command);
                        case "EXISTS" -> handleExists(command);
                        case "EXPIRE" -> handleExpire(command);
                        case "EXPIREAT" -> handleExpireAt(command);
                        case "TTL" -> handleTtl(command);
                        default -> {
                            log.warn("Received unknown command: {}", command.name());
                            yield RespValue.Error.err("unknown command '" + command.name() + "'");
                        }
                    };

            log.debug("Command {} complete with result: {}", command.name(), result);
            return result;

        } catch (ArityException e) {
            log.warn("Arity error for command {}: {}", command.name(), e.getMessage());
            return RespValue.Error.err(e.getMessage());
        }
    }

    private RespValue handleSet(Command command) {
        String key = command.requireString(0);
        String value = command.requireString(1);

        storageEngine.set(key, value);
        appendAof(command);

        log.debug("SET '{}' = '{}'", key, value);
        return new RespValue.SimpleString("OK");
    }

    private RespValue handleGet(Command command) {
        String key = command.requireString(0);
        String value = storageEngine.get(key);

        if (value == null) {
            log.debug("GET '{}' -> (nil)", key);
        } else {
            log.debug("GET '{}' -> '{}'", key, value);
        }

        return new RespValue.BulkString(value);
    }

    private RespValue handleDel(Command command) {
        String key = command.requireString(0);
        boolean deleted = storageEngine.delete(key);

        if (deleted) {
            appendAof(command);
            log.debug("DEL '{}' -> deleted", key);
        } else {
            log.debug("DEL '{}' -> not found", key);
        }

        return new RespValue.Integer(deleted ? 1 : 0);
    }

    private RespValue handleExists(Command command) {
        String key = command.requireString(0);
        boolean exists = storageEngine.exists(key);

        log.debug("EXISTS '{}' -> {}", key, exists);

        return new RespValue.Integer(exists ? 1 : 0);
    }

    private RespValue handleExpire(Command command) {
        String key = command.requireString(0);
        String seconds = command.requireString(1);

        try {
            long ttl = Long.parseLong(seconds);
            long absoluteTtl = System.currentTimeMillis() + ttl * 1000;
            boolean updated = storageEngine.expire(key, absoluteTtl);

            if (updated) {
                List<RespValue> aofCommand =
                        List.of(
                                new RespValue.BulkString(key),
                                new RespValue.BulkString(String.valueOf(absoluteTtl / 1000)));

                appendAof(new Command("EXPIREAT", aofCommand));
                log.debug("EXPIRE '{}' in {}s (at epoch ms {})", key, ttl, absoluteTtl);
            } else {
                log.debug("EXPIRE '{}' -> not found", key);
            }

            return new RespValue.Integer(updated ? 1 : 0);
        } catch (NumberFormatException e) {
            log.warn("EXPIRE received non-integer TTL: '{}'", seconds);
            return RespValue.Error.err("value is not an integer or out of range");
        }
    }

    private RespValue handleExpireAt(Command command) {
        String key = command.requireString(0);
        String epochStr = command.requireString(1);

        try {
            long epochSeconds = Long.parseLong(epochStr);
            long epochMillis = epochSeconds * 1000;
            boolean updated = storageEngine.expire(key, epochMillis);

            if (updated) {
                log.debug("EXPIREAT '{}' at epoch ms {}", key, epochMillis);
            } else {
                log.debug("EXPIREAT '{}' -> key not found", key);
            }

            return new RespValue.Integer(updated ? 1 : 0);
        } catch (NumberFormatException e) {
            log.warn("EXPIREAT received non-integer epoch: '{}'", epochStr);
            return RespValue.Error.err("value is not an integer or out of range");
        }
    }

    private RespValue handleTtl(Command command) {
        String key = command.requireString(0);
        return new RespValue.Integer(storageEngine.getTtl(key));
    }

    private void appendAof(Command command) {
        if (aofWriter == null) return;

        List<RespValue> full = new ArrayList<>();
        full.add(new RespValue.BulkString(command.name()));
        full.addAll(command.args());

        try {
            aofWriter.append(new RespValue.Array(full));
        } catch (AofQueueFullException e) {
            log.error("AOF queue full, write may be lost for command: {}", command);
            throw e;
        }
    }
}
