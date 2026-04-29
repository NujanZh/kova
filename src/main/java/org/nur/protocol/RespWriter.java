package org.nur.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespWriter {

    private final OutputStream output;

    public RespWriter(OutputStream output) {
        this.output = output;
    }

    public void write(RespValue value) throws IOException {
        output.write(serialize(value).getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    private String serialize(RespValue value) {
        return switch (value) {
            case RespValue.SimpleString simpleString -> serializeSimpleString(simpleString);
            case RespValue.BulkString bulkString -> serializeBulkString(bulkString);
            case RespValue.Integer integer -> serializeInteger(integer);
            case RespValue.Error error -> serializeError(error);
            case RespValue.Array array -> serializeArray(array);
        };
    }

    private String serializeSimpleString(RespValue.SimpleString value) {
        return "+" + value.value() + "\r\n";
    }

    private String serializeBulkString(RespValue.BulkString value) {
        if (value.value() == null) {
            return "$-1\r\n";
        }

        int len = value.value().getBytes(StandardCharsets.UTF_8).length;
        return "$" + len + "\r\n" + value.value() + "\r\n";
    }

    private String serializeInteger(RespValue.Integer value) {
        return ":" + value.value() + "\r\n";
    }

    private String serializeError(RespValue.Error value) {
        return "-" + value.code() + " " + value.message() + "\r\n";
    }

    private String serializeArray(RespValue.Array value) {
        if (value.elements() == null) {
            return "*-1\r\n";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(value.elements().size()).append("\r\n");
        for (RespValue element : value.elements()) {
            sb.append(serialize(element));
        }
        return sb.toString();
    }
}
