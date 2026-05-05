package org.nur.persistence;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.nur.commands.CommandHandler;
import org.nur.protocol.RespSerializer;
import org.nur.protocol.RespValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class AofLoaderTest {

    @Mock private CommandHandler commandHandler;

    private Path aofFile;
    private AofLoader loader;

    @BeforeEach
    void setUp() throws IOException {
        aofFile = Files.createTempFile("aof-loader-test-", ".log");
        loader = new AofLoader(commandHandler);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(aofFile);
    }

    private RespValue.Array command(String... parts) {
        List<RespValue> elements =
                java.util.Arrays.stream(parts)
                        .map(RespValue.BulkString::new)
                        .map(v -> (RespValue) v)
                        .toList();
        return new RespValue.Array(elements);
    }

    private void writeToAof(RespValue... commands) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (RespValue cmd : commands) {
            sb.append(RespSerializer.serialize(cmd));
        }
        Files.writeString(aofFile, sb.toString(), StandardCharsets.UTF_8);
    }

    private void appendRawToAof(String raw) throws IOException {
        Files.writeString(
                aofFile, raw, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
    }

    @Test
    void emptyFileReplayesNoCommands() throws IOException {
        loader.load(aofFile);
        verify(commandHandler, never()).handle(any());
    }

    @Test
    void replaysSingleCommand() throws IOException {
        when(commandHandler.handle(any())).thenReturn(new RespValue.SimpleString("OK"));

        writeToAof(command("SET", "foo", "bar"));
        loader.load(aofFile);

        verify(commandHandler, times(1)).handle(any());
    }

    @Test
    void replaysMultipleCommands() throws IOException {
        when(commandHandler.handle(any())).thenReturn(new RespValue.SimpleString("OK"));

        writeToAof(
                command("SET", "k1", "v1"), command("SET", "k2", "v2"), command("SET", "k3", "v3"));

        loader.load(aofFile);

        verify(commandHandler, times(3)).handle(any());
    }

    @Test
    void replaysCommandsInOrder() throws IOException {
        var inOrder = inOrder(commandHandler);
        when(commandHandler.handle(any())).thenReturn(new RespValue.SimpleString("OK"));

        RespValue.Array first = command("SET", "first", "1");
        RespValue.Array second = command("SET", "second", "2");
        RespValue.Array third = command("DEL", "first");

        writeToAof(first, second, third);
        loader.load(aofFile);

        inOrder.verify(commandHandler).handle(first);
        inOrder.verify(commandHandler).handle(second);
        inOrder.verify(commandHandler).handle(third);
    }

    @Test
    void continuesReplayWhenCommandReturnsError() throws IOException {
        when(commandHandler.handle(any()))
                .thenReturn(RespValue.Error.err("something failed"))
                .thenReturn(new RespValue.SimpleString("OK"));

        writeToAof(command("SET", "bad", "val"), command("SET", "good", "val"));
        loader.load(aofFile);

        verify(commandHandler, times(2)).handle(any());
    }

    @Test
    void stopsReplayOnCorruptedFile() throws IOException {
        when(commandHandler.handle(any())).thenReturn(new RespValue.SimpleString("OK"));

        writeToAof(command("SET", "foo", "bar"));
        appendRawToAof("!!! this is not valid RESP !!!\r\n");

        loader.load(aofFile);

        verify(commandHandler, times(1)).handle(any());
    }

    @Test
    void stopsReplayOnCorruptedFileWithNoValidCommands() throws IOException {
        Files.writeString(aofFile, "garbage data\r\n", StandardCharsets.UTF_8);

        loader.load(aofFile);

        verify(commandHandler, never()).handle(any());
    }

    @Test
    void handlesPartiallyWrittenFinalCommand() throws IOException {
        when(commandHandler.handle(any())).thenReturn(new RespValue.SimpleString("OK"));

        writeToAof(command("SET", "k1", "v1"), command("SET", "k2", "v2"));
        appendRawToAof("*3\r\n$3\r\nSET\r\n$2\r\nk3"); // truncated — no value or trailing CRLF

        loader.load(aofFile);

        verify(commandHandler, times(2)).handle(any());
    }

    @Test
    void throwsIOExceptionForNonExistentFile() {
        Path missing = Path.of("/nonexistent/path/aof.log");
        assertThrows(IOException.class, () -> loader.load(missing));
    }

    @Test
    void doesNotThrowForValidFile() throws IOException {
        when(commandHandler.handle(any())).thenReturn(new RespValue.SimpleString("OK"));
        writeToAof(command("SET", "foo", "bar"));
        assertDoesNotThrow(() -> loader.load(aofFile));
    }
}
