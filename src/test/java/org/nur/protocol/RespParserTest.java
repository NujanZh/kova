package org.nur.protocol;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.nur.exception.ClientDisconnectedException;
import org.nur.exception.RespProtocolException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

class RespParserTest {

    private RespParser parserFor(String input) {
        InputStream stream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        return new RespParser(stream);
    }

    @Test
    void parsesSimpleString() {
        RespValue result = parserFor("+OK\r\n").parse();
        assertEquals(new RespValue.SimpleString("OK"), result);
    }

    @Test
    void parsesEmptySimpleString() {
        RespValue result = parserFor("+\r\n").parse();
        assertEquals(new RespValue.SimpleString(""), result);
    }

    @Test
    void parsesSimpleStringWithSpaces() {
        RespValue result = parserFor("+hello world\r\n").parse();
        assertEquals(new RespValue.SimpleString("hello world"), result);
    }

    @Test
    void parsesError() {
        RespValue result = parserFor("-ERR something went wrong\r\n").parse();
        assertInstanceOf(RespValue.Error.class, result);
        RespValue.Error error = (RespValue.Error) result;
        assertEquals("ERR", error.code());
        assertEquals("something went wrong", error.message());
    }

    @Test
    void parsesErrorWithNoMessage() {
        RespValue result = parserFor("-WRONGTYPE\r\n").parse();
        assertInstanceOf(RespValue.Error.class, result);
        RespValue.Error error = (RespValue.Error) result;
        assertEquals("WRONGTYPE", error.code());
        assertEquals("", error.message());
    }

    @Test
    void parsesPositiveInteger() {
        RespValue result = parserFor(":42\r\n").parse();
        assertEquals(new RespValue.Integer(42), result);
    }

    @Test
    void parsesNegativeInteger() {
        RespValue result = parserFor(":-1\r\n").parse();
        assertEquals(new RespValue.Integer(-1), result);
    }

    @Test
    void parsesZeroInteger() {
        RespValue result = parserFor(":0\r\n").parse();
        assertEquals(new RespValue.Integer(0), result);
    }

    @Test
    void parsesBulkString() {
        RespValue result = parserFor("$5\r\nhello\r\n").parse();
        assertEquals(new RespValue.BulkString("hello"), result);
    }

    @Test
    void parsesEmptyBulkString() {
        RespValue result = parserFor("$0\r\n\r\n").parse();
        assertEquals(new RespValue.BulkString(""), result);
    }

    @Test
    void parsesNullBulkString() {
        RespValue result = parserFor("$-1\r\n").parse();
        assertEquals(new RespValue.BulkString(null), result);
    }

    @Test
    void parsesBulkStringWithSpaces() {
        RespValue result = parserFor("$11\r\nhello world\r\n").parse();
        assertEquals(new RespValue.BulkString("hello world"), result);
    }

    @Test
    void parsesBulkStringWithNewlines() {
        String content = "hello\r\nworld";
        String raw = "$12\r\n" + content + "\r\n";
        RespValue result = parserFor(raw).parse();
        assertEquals(new RespValue.BulkString(content), result);
    }

    @Test
    void parsesEmptyArray() {
        RespValue result = parserFor("*0\r\n").parse();
        assertInstanceOf(RespValue.Array.class, result);
        RespValue.Array array = (RespValue.Array) result;
        assertNotNull(array.elements());
        assertTrue(array.elements().isEmpty());
    }

    @Test
    void parsesNullArray() {
        RespValue result = parserFor("*-1\r\n").parse();
        assertInstanceOf(RespValue.Array.class, result);
        assertNull(((RespValue.Array) result).elements());
    }

    @Test
    void parsesSimpleCommandArray() {
        String raw = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        RespValue result = parserFor(raw).parse();

        assertInstanceOf(RespValue.Array.class, result);
        List<RespValue> elements = ((RespValue.Array) result).elements();
        assertEquals(3, elements.size());
        assertEquals(new RespValue.BulkString("SET"), elements.get(0));
        assertEquals(new RespValue.BulkString("foo"), elements.get(1));
        assertEquals(new RespValue.BulkString("bar"), elements.get(2));
    }

    @Test
    void parsesNestedArray() {
        String raw = "*2\r\n*1\r\n:1\r\n+OK\r\n";
        RespValue result = parserFor(raw).parse();

        assertInstanceOf(RespValue.Array.class, result);
        List<RespValue> outer = ((RespValue.Array) result).elements();
        assertEquals(2, outer.size());

        assertInstanceOf(RespValue.Array.class, outer.getFirst());
        List<RespValue> inner = ((RespValue.Array) outer.getFirst()).elements();
        assertEquals(1, inner.size());
        assertEquals(new RespValue.Integer(1), inner.getFirst());

        assertEquals(new RespValue.SimpleString("OK"), outer.get(1));
    }

    @Test
    void parsesArrayWithMixedTypes() {
        String raw = "*3\r\n$3\r\nfoo\r\n:100\r\n+bar\r\n";
        RespValue result = parserFor(raw).parse();

        List<RespValue> elements = ((RespValue.Array) result).elements();
        assertEquals(new RespValue.BulkString("foo"), elements.get(0));
        assertEquals(new RespValue.Integer(100), elements.get(1));
        assertEquals(new RespValue.SimpleString("bar"), elements.get(2));
    }

    @Test
    void throwsClientDisconnectedOnEmptyStream() {
        RespParser parser = parserFor("");
        assertThrows(ClientDisconnectedException.class, parser::parse);
    }

    @ParameterizedTest
    @ValueSource(strings = {":abc\r\n", "!unknown\r\n", "+OK\n"})
    void throwsRespProtocolException(String input) {
        RespParser parser = parserFor(input);
        assertThrows(RespProtocolException.class, parser::parse);
    }
}
