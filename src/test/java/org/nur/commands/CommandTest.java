package org.nur.commands;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.nur.exception.ArityException;
import org.nur.protocol.RespValue;

import java.util.List;

class CommandTest {

    @Test
    void parsesCommandNameAsUpperCase() {
        List<RespValue> elements =
                List.of(
                        new RespValue.BulkString("set"),
                        new RespValue.BulkString("foo"),
                        new RespValue.BulkString("bar"));

        Command command = Command.parse(elements);

        assertNotNull(command);
        assertEquals("SET", command.name());
    }

    @Test
    void parsesArgsExcludingCommandName() {
        List<RespValue> elements =
                List.of(
                        new RespValue.BulkString("SET"),
                        new RespValue.BulkString("foo"),
                        new RespValue.BulkString("bar"));

        Command command = Command.parse(elements);

        assertNotNull(command);
        assertEquals(2, command.args().size());
        assertEquals(new RespValue.BulkString("foo"), command.args().get(0));
        assertEquals(new RespValue.BulkString("bar"), command.args().get(1));
    }

    @Test
    void parsesCommandWithNoArgs() {
        List<RespValue> elements = List.of(new RespValue.BulkString("PING"));

        Command command = Command.parse(elements);

        assertNotNull(command);
        assertEquals("PING", command.name());
        assertTrue(command.args().isEmpty());
    }

    @Test
    void returnsNullWhenFirstElementIsNotBulkString() {
        List<RespValue> elements =
                List.of(new RespValue.Integer(1), new RespValue.BulkString("foo"));

        Command command = Command.parse(elements);

        assertNull(command);
    }

    @Test
    void returnsNullWhenFirstElementIsSimpleString() {
        List<RespValue> elements = List.of(new RespValue.SimpleString("SET"));

        Command command = Command.parse(elements);

        assertNull(command);
    }

    @Test
    void returnsNullWhenFirstElementIsNullBulkString() {
        List<RespValue> elements = List.of(new RespValue.BulkString(null));

        Command command = Command.parse(elements);

        assertNull(command);
    }

    @Test
    void mixedCaseCommandNormalisedToUpperCase() {
        List<RespValue> elements = List.of(new RespValue.BulkString("eXpIrE"));

        Command command = Command.parse(elements);

        assertNotNull(command);
        assertEquals("EXPIRE", command.name());
    }

    @Test
    void requireStringReturnsValueAtIndex() {
        Command command =
                Command.parse(
                        List.of(
                                new RespValue.BulkString("SET"),
                                new RespValue.BulkString("mykey"),
                                new RespValue.BulkString("myvalue")));

        assertNotNull(command);
        assertEquals("mykey", command.requireString(0));
        assertEquals("myvalue", command.requireString(1));
    }

    @Test
    void requireStringThrowsArityExceptionWhenIndexOutOfBounds() {
        Command command =
                Command.parse(
                        List.of(new RespValue.BulkString("GET"), new RespValue.BulkString("foo")));

        assertNotNull(command);
        assertThrows(ArityException.class, () -> command.requireString(1));
    }

    @Test
    void requireStringThrowsArityExceptionForEmptyArgs() {
        Command command = Command.parse(List.of(new RespValue.BulkString("PING")));

        assertNotNull(command);
        assertThrows(ArityException.class, () -> command.requireString(0));
    }

    @Test
    void requireStringThrowsArityExceptionWhenArgIsNotBulkString() {
        Command command = new Command("SET", List.of(new RespValue.Integer(42)));

        assertThrows(ArityException.class, () -> command.requireString(0));
    }

    @Test
    void arityExceptionMessageContainsCommandName() {
        Command command = Command.parse(List.of(new RespValue.BulkString("GET")));

        assertNotNull(command);
        ArityException ex = assertThrows(ArityException.class, () -> command.requireString(0));
        assertTrue(ex.getMessage().contains("GET"), "Expected message to contain command name");
    }

    @Test
    void requireStringWorksForLastValidIndex() {
        Command command =
                Command.parse(
                        List.of(
                                new RespValue.BulkString("EXPIRE"),
                                new RespValue.BulkString("mykey"),
                                new RespValue.BulkString("30")));

        assertNotNull(command);
        assertEquals("30", command.requireString(1));
    }
}
