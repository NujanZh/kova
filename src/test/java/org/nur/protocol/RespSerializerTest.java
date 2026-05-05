package org.nur.protocol;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

class RespSerializerTest {

    @Test
    void serializesSimpleString() {
        String result = RespSerializer.serialize(new RespValue.SimpleString("OK"));
        assertEquals("+OK\r\n", result);
    }

    @Test
    void serializesEmptySimpleString() {
        String result = RespSerializer.serialize(new RespValue.SimpleString(""));
        assertEquals("+\r\n", result);
    }

    @Test
    void serializesSimpleStringWithSpaces() {
        String result = RespSerializer.serialize(new RespValue.SimpleString("hello world"));
        assertEquals("+hello world\r\n", result);
    }

    @Test
    void serializesError() {
        String result =
                RespSerializer.serialize(new RespValue.Error("ERR", "something went wrong"));
        assertEquals("-ERR something went wrong\r\n", result);
    }

    @Test
    void serializesErrorWithEmptyMessage() {
        String result = RespSerializer.serialize(new RespValue.Error("WRONGTYPE", ""));
        assertEquals("-WRONGTYPE \r\n", result);
    }

    @Test
    void serializesErrorFactoryMethod() {
        String result = RespSerializer.serialize(RespValue.Error.err("bad input"));
        assertEquals("-ERR bad input\r\n", result);
    }

    @Test
    void serializesPositiveInteger() {
        String result = RespSerializer.serialize(new RespValue.Integer(42));
        assertEquals(":42\r\n", result);
    }

    @Test
    void serializesNegativeInteger() {
        String result = RespSerializer.serialize(new RespValue.Integer(-1));
        assertEquals(":-1\r\n", result);
    }

    @Test
    void serializesZeroInteger() {
        String result = RespSerializer.serialize(new RespValue.Integer(0));
        assertEquals(":0\r\n", result);
    }

    @Test
    void serializesBulkString() {
        String result = RespSerializer.serialize(new RespValue.BulkString("hello"));
        assertEquals("$5\r\nhello\r\n", result);
    }

    @Test
    void serializesEmptyBulkString() {
        String result = RespSerializer.serialize(new RespValue.BulkString(""));
        assertEquals("$0\r\n\r\n", result);
    }

    @Test
    void serializesNullBulkString() {
        String result = RespSerializer.serialize(new RespValue.BulkString(null));
        assertEquals("$-1\r\n", result);
    }

    @Test
    void serializesBulkStringLengthIsByteCount() {
        String result = RespSerializer.serialize(new RespValue.BulkString("é"));
        assertEquals("$2\r\né\r\n", result);
    }

    @Test
    void serializesEmptyArray() {
        String result = RespSerializer.serialize(new RespValue.Array(List.of()));
        assertEquals("*0\r\n", result);
    }

    @Test
    void serializesNullArray() {
        String result = RespSerializer.serialize(new RespValue.Array(null));
        assertEquals("*-1\r\n", result);
    }

    @Test
    void serializesSimpleCommandArray() {
        RespValue array =
                new RespValue.Array(
                        List.of(
                                new RespValue.BulkString("SET"),
                                new RespValue.BulkString("foo"),
                                new RespValue.BulkString("bar")));

        String result = RespSerializer.serialize(array);
        assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", result);
    }

    @Test
    void serializesMixedTypeArray() {
        RespValue array =
                new RespValue.Array(
                        List.of(
                                new RespValue.SimpleString("OK"),
                                new RespValue.Integer(7),
                                new RespValue.BulkString(null)));

        String result = RespSerializer.serialize(array);
        assertEquals("*3\r\n+OK\r\n:7\r\n$-1\r\n", result);
    }

    @Test
    void serializesNestedArray() {
        RespValue inner = new RespValue.Array(List.of(new RespValue.Integer(1)));
        RespValue outer = new RespValue.Array(List.of(inner, new RespValue.SimpleString("OK")));

        String result = RespSerializer.serialize(outer);
        assertEquals("*2\r\n*1\r\n:1\r\n+OK\r\n", result);
    }

    @Test
    void roundtripBulkString() {
        RespValue original = new RespValue.BulkString("hello world");
        String serialized = RespSerializer.serialize(original);
        RespValue parsed =
                new RespParser(
                                new ByteArrayInputStream(
                                        serialized.getBytes(
                                                java.nio.charset.StandardCharsets.UTF_8)))
                        .parse();
        assertEquals(original, parsed);
    }

    @Test
    void roundtripCommandArray() {
        RespValue original =
                new RespValue.Array(
                        List.of(
                                new RespValue.BulkString("GET"),
                                new RespValue.BulkString("mykey")));
        String serialized = RespSerializer.serialize(original);
        RespValue parsed =
                new RespParser(
                                new ByteArrayInputStream(
                                        serialized.getBytes(
                                                java.nio.charset.StandardCharsets.UTF_8)))
                        .parse();
        assertEquals(original, parsed);
    }
}
