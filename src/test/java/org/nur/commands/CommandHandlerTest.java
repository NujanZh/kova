package org.nur.commands;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.nur.persistence.AofWriter;
import org.nur.protocol.RespValue;
import org.nur.storage.StorageEngine;

import java.util.Arrays;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class CommandHandlerTest {

    @Mock private StorageEngine storageEngine;

    @Mock private AofWriter aofWriter;

    private CommandHandler handler;
    private CommandHandler handlerNoAof;

    @BeforeEach
    void setUp() {
        handler = new CommandHandler(storageEngine, aofWriter);
        handlerNoAof = new CommandHandler(storageEngine);
    }

    private RespValue command(String... parts) {
        List<RespValue> elements =
                Arrays.stream(parts)
                        .map(RespValue.BulkString::new)
                        .map(b -> (RespValue) b)
                        .toList();
        return new RespValue.Array(elements);
    }

    private RespValue.Error assertError(RespValue result) {
        assertInstanceOf(RespValue.Error.class, result);
        return (RespValue.Error) result;
    }

    @Test
    void rejectsMalformedInputNotAnArray() {
        RespValue result = handler.handle(new RespValue.SimpleString("garbage"));
        assertError(result);
    }

    @Test
    void rejectsEmptyArray() {
        RespValue result = handler.handle(new RespValue.Array(List.of()));
        assertError(result);
    }

    @Test
    void rejectsNullElementArray() {
        RespValue result = handler.handle(new RespValue.Array(null));
        assertError(result);
    }

    @Test
    void rejectsCommandWithNonBulkStringName() {
        RespValue result = handler.handle(new RespValue.Array(List.of(new RespValue.Integer(1))));
        assertError(result);
    }

    @Test
    void pingReturnsPong() {
        RespValue result = handler.handle(command("PING"));
        assertEquals(new RespValue.SimpleString("PONG"), result);
    }

    @Test
    void pingIsCaseInsensitive() {
        RespValue result = handler.handle(command("ping"));
        assertEquals(new RespValue.SimpleString("PONG"), result);
    }

    @Test
    void setCallsStorageAndReturnsOk() {
        RespValue result = handler.handle(command("SET", "foo", "bar"));

        verify(storageEngine).set("foo", "bar");
        assertEquals(new RespValue.SimpleString("OK"), result);
    }

    @Test
    void setAppendsToAof() {
        handler.handle(command("SET", "foo", "bar"));
        verify(aofWriter).append(any(RespValue.Array.class));
    }

    @Test
    void setWithMissingValueReturnsArityError() {
        RespValue result = handler.handle(command("SET", "foo"));
        RespValue.Error error = assertError(result);
        assertEquals("ERR", error.code());
        assertTrue(error.message().contains("SET"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SET", "DEL", "GET", "EXISTS"})
    void commandWithNoArgsReturnsArityError(String commandName) {
        RespValue result = handler.handle(command(commandName));
        assertError(result);
    }

    @Test
    void getReturnsValueFromStorage() {
        when(storageEngine.get("foo")).thenReturn("bar");

        RespValue result = handler.handle(command("GET", "foo"));

        assertEquals(new RespValue.BulkString("bar"), result);
    }

    @Test
    void getReturnsNullBulkStringForMissingKey() {
        when(storageEngine.get("missing")).thenReturn(null);

        RespValue result = handler.handle(command("GET", "missing"));

        assertEquals(new RespValue.BulkString(null), result);
    }

    @Test
    void getDoesNotAppendToAof() {
        when(storageEngine.get(any())).thenReturn("val");

        handler.handle(command("GET", "foo"));

        verify(aofWriter, never()).append(any());
    }

    @Test
    void delReturnsOneWhenKeyDeleted() {
        when(storageEngine.delete("foo")).thenReturn(true);

        RespValue result = handler.handle(command("DEL", "foo"));

        assertEquals(new RespValue.Integer(1), result);
    }

    @Test
    void delReturnsZeroWhenKeyMissing() {
        when(storageEngine.delete("foo")).thenReturn(false);

        RespValue result = handler.handle(command("DEL", "foo"));

        assertEquals(new RespValue.Integer(0), result);
    }

    @Test
    void delAppendsToAofOnlyWhenDeleted() {
        when(storageEngine.delete("foo")).thenReturn(true);
        handler.handle(command("DEL", "foo"));
        verify(aofWriter).append(any());

        when(storageEngine.delete("bar")).thenReturn(false);
        handler.handle(command("DEL", "bar"));
        verify(aofWriter, times(1)).append(any());
    }

    @Test
    void existsReturnsOneWhenKeyPresent() {
        when(storageEngine.exists("foo")).thenReturn(true);

        RespValue result = handler.handle(command("EXISTS", "foo"));

        assertEquals(new RespValue.Integer(1), result);
    }

    @Test
    void existsReturnsZeroWhenKeyAbsent() {
        when(storageEngine.exists("foo")).thenReturn(false);

        RespValue result = handler.handle(command("EXISTS", "foo"));

        assertEquals(new RespValue.Integer(0), result);
    }

    @Test
    void expireReturnsOneWhenKeyUpdated() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(true);

        RespValue result = handler.handle(command("EXPIRE", "foo", "10"));

        assertEquals(new RespValue.Integer(1), result);
    }

    @Test
    void expireReturnsZeroWhenKeyMissing() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(false);

        RespValue result = handler.handle(command("EXPIRE", "foo", "10"));

        assertEquals(new RespValue.Integer(0), result);
    }

    @Test
    void expireConvertsToAbsoluteEpochMillis() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(true);

        long before = System.currentTimeMillis();
        handler.handle(command("EXPIRE", "foo", "10"));
        long after = System.currentTimeMillis();

        ArgumentCaptor<Long> epochCaptor = ArgumentCaptor.forClass(Long.class);
        verify(storageEngine).expire(eq("foo"), epochCaptor.capture());

        long captured = epochCaptor.getValue();
        assertTrue(captured >= before + 10_000, "Expected epoch >= now+10s, got: " + captured);
        assertTrue(captured <= after + 10_000, "Expected epoch <= now+10s, got: " + captured);
    }

    @Test
    void expireAppendsExpireAtCommandToAof() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(true);
        handler.handle(command("EXPIRE", "foo", "10"));

        ArgumentCaptor<RespValue.Array> captor = ArgumentCaptor.forClass(RespValue.Array.class);
        verify(aofWriter).append(captor.capture());

        RespValue.BulkString commandName =
                (RespValue.BulkString) captor.getValue().elements().getFirst();
        assertEquals("EXPIREAT", commandName.value());
    }

    @Test
    void expireDoesNotAppendToAofWhenKeyMissing() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(false);
        handler.handle(command("EXPIRE", "foo", "10"));
        verify(aofWriter, never()).append(any());
    }

    @Test
    void expireWithNonIntegerTtlReturnsError() {
        RespValue result = handler.handle(command("EXPIRE", "foo", "notanumber"));
        RespValue.Error error = assertError(result);
        assertTrue(error.message().contains("integer"));
    }

    @Test
    void expireWithMissingTtlReturnsArityError() {
        RespValue result = handler.handle(command("EXPIRE", "foo"));
        assertError(result);
    }

    @Test
    void expireAtPassesEpochMillisToStorage() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(true);

        handler.handle(command("EXPIREAT", "foo", "9999999999"));

        verify(storageEngine).expire("foo", 9999999999L * 1000);
    }

    @Test
    void expireAtReturnsOneWhenKeyUpdated() {
        when(storageEngine.expire(eq("foo"), anyLong())).thenReturn(true);

        RespValue result = handler.handle(command("EXPIREAT", "foo", "9999999999"));

        assertEquals(new RespValue.Integer(1), result);
    }

    @Test
    void expireAtWithNonIntegerEpochReturnsError() {
        RespValue result = handler.handle(command("EXPIREAT", "foo", "abc"));
        RespValue.Error error = assertError(result);
        assertTrue(error.message().contains("integer"));
    }

    @Test
    void ttlReturnsMinus2ForMissingKey() {
        when(storageEngine.getTtl("foo")).thenReturn(-2L);

        RespValue result = handler.handle(command("TTL", "foo"));

        assertEquals(new RespValue.Integer(-2), result);
    }

    @Test
    void ttlReturnsMinus1ForKeyWithNoExpiry() {
        when(storageEngine.getTtl("foo")).thenReturn(-1L);

        RespValue result = handler.handle(command("TTL", "foo"));

        assertEquals(new RespValue.Integer(-1), result);
    }

    @Test
    void ttlReturnsRemainingSeconds() {
        when(storageEngine.getTtl("foo")).thenReturn(42L);

        RespValue result = handler.handle(command("TTL", "foo"));

        assertEquals(new RespValue.Integer(42), result);
    }

    @Test
    void unknownCommandReturnsError() {
        RespValue result = handler.handle(command("FLUSHALL"));
        RespValue.Error error = assertError(result);
        assertTrue(error.message().contains("FLUSHALL"));
    }

    @Test
    void setWithNoAofDoesNotThrow() {
        assertDoesNotThrow(() -> handlerNoAof.handle(command("SET", "foo", "bar")));
        verify(storageEngine).set("foo", "bar");
    }

    @Test
    void delWithNoAofDoesNotThrow() {
        when(storageEngine.delete("foo")).thenReturn(true);
        assertDoesNotThrow(() -> handlerNoAof.handle(command("DEL", "foo")));
    }
}
