package org.nur.protocol;

import java.util.List;

public sealed interface RespValue
        permits RespValue.SimpleString,
                RespValue.Error,
                RespValue.Integer,
                RespValue.BulkString,
                RespValue.Array {
    record SimpleString(String value) implements RespValue {}

    record Error(String code, String message) implements RespValue {
        static Error of(String raw) {
            int space = raw.indexOf(' ');
            if (space == -1) return new Error(raw, "");
            return new Error(raw.substring(0, space), raw.substring(space + 1));
        }

        static Error err(String message) {
            return new Error("ERR", message);
        }

        static Error wrongType() {
            return new Error(
                    "WRONGTYPE", "Operation against a key holding the wrong kind of value");
        }
    }

    record Integer(long value) implements RespValue {}

    record BulkString(String value) implements RespValue {}

    record Array(List<RespValue> elements) implements RespValue {}
}
