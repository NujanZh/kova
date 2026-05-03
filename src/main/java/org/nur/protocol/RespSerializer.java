package org.nur.protocol;

import java.nio.charset.StandardCharsets;

public class RespSerializer {

    public static String serialize(RespValue value) {
        return switch (value) {
            case RespValue.SimpleString simpleString -> serializeSimpleString(simpleString);
            case RespValue.BulkString bulkString -> serializeBulkString(bulkString);
            case RespValue.Integer integer -> serializeInteger(integer);
            case RespValue.Error error -> serializeError(error);
            case RespValue.Array array -> serializeArray(array);
        };
    }

    private static String serializeSimpleString(RespValue.SimpleString value) {
        return "+" + value.value() + "\r\n";
    }

    private static String serializeBulkString(RespValue.BulkString value) {
        if (value.value() == null) {
            return "$-1\r\n";
        }

        int len = value.value().getBytes(StandardCharsets.UTF_8).length;
        return "$" + len + "\r\n" + value.value() + "\r\n";
    }

    private static String serializeInteger(RespValue.Integer value) {
        return ":" + value.value() + "\r\n";
    }

    private static String serializeError(RespValue.Error value) {
        return "-" + value.code() + " " + value.message() + "\r\n";
    }

    private static String serializeArray(RespValue.Array value) {
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
