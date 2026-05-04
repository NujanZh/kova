package org.nur.protocol;

import org.nur.exception.ClientDisconnectedException;
import org.nur.exception.RespProtocolException;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespParser {
    private final BufferedInputStream reader;

    public RespParser(InputStream inputStream) {
        this.reader = new BufferedInputStream(inputStream);
    }

    public RespValue parse() {
        try {
            int type = reader.read();

            return switch (type) {
                case '*' -> readArray();
                case '$' -> readBulkString();
                case ':' -> readInteger();
                case '+' -> readSimpleString();
                case '-' -> readError();
                case -1 -> throw new ClientDisconnectedException();
                default -> throw new RespProtocolException("Unexpected value: " + type);
            };
        } catch (IOException e) {
            throw new RespProtocolException(e.getMessage());
        }
    }

    private RespValue.Array readArray() {
        int len = readLength();

        if (len == -1) {
            return new RespValue.Array(null);
        }

        List<RespValue> elements = new ArrayList<>(len);

        for (int i = 0; i < len; i++) {
            elements.add(parse());
        }

        return new RespValue.Array(elements);
    }

    private RespValue.BulkString readBulkString() {
        int len = readLength();

        if (len == -1) {
            return new RespValue.BulkString(null);
        }

        byte[] data = new byte[len];

        try {
            int totalRead = 0;

            while (totalRead < len) {
                int read = reader.read(data, totalRead, len - totalRead);
                if (read == -1) {
                    throw new RespProtocolException("Stream ended before reading full bulk string");
                }
                totalRead += read;
            }
        } catch (IOException e) {
            throw new RespProtocolException(e.getMessage());
        }

        readLine();

        return new RespValue.BulkString(new String(data, StandardCharsets.UTF_8));
    }

    private RespValue.SimpleString readSimpleString() {
        byte[] line = readLine();
        return new RespValue.SimpleString(new String(line, StandardCharsets.UTF_8));
    }

    private RespValue.Error readError() {
        byte[] line = readLine();
        return RespValue.Error.of(new String(line, StandardCharsets.UTF_8));
    }

    private RespValue.Integer readInteger() {
        return new RespValue.Integer(parseNumber(readLine()));
    }

    private int readLength() {
        return (int) parseNumber(readLine());
    }

    private long parseNumber(byte[] line) {
        if (line.length == 0) {
            throw new RespProtocolException("Empty line");
        }

        long result = 0;
        boolean negative = false;
        int start = 0;

        if (line[0] == '-') {
            negative = true;
            start = 1;
        }

        for (int i = start; i < line.length; i++) {
            byte b = line[i];
            if (b < '0' || b > '9') {
                throw new RespProtocolException("Invalid character in number: " + (char) b);
            }

            result = result * 10 + (b - '0');
        }

        return negative ? -result : result;
    }

    private byte[] readLine() {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int b;
            boolean seenAnyByte = false;

            while ((b = reader.read()) != -1) {
                seenAnyByte = true;
                if (b == '\r') {
                    int next = reader.read();
                    if (next == '\n') {
                        break;
                    } else {
                        throw new RespProtocolException(
                                "Expected \\n after \\r, got " + (char) next);
                    }
                }
                buffer.write(b);
            }

            if (!seenAnyByte) {
                throw new RespProtocolException("Stream ended before reading line");
            }

            return buffer.toByteArray();
        } catch (IOException e) {
            throw new RespProtocolException(e.getMessage());
        }
    }
}
