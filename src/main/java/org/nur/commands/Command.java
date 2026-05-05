package org.nur.commands;

import org.nur.exception.ArityException;
import org.nur.protocol.RespValue;

import java.util.List;

public record Command(String name, List<RespValue> args) {

    public static Command parse(List<RespValue> elements) {
        if (!(elements.getFirst() instanceof RespValue.BulkString(String name))) {
            return null;
        }

        return new Command(name.toUpperCase(), elements.subList(1, elements.size()));
    }

    public String requireString(int index) {
        if (index >= args.size()) {
            throw new ArityException(this.name);
        }

        if (!(args.get(index) instanceof RespValue.BulkString(String value))) {
            throw new ArityException(this.name);
        }

        return value;
    }
}
