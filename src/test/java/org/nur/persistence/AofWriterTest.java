package org.nur.persistence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nur.exception.AofQueueFullException;
import org.nur.exception.AofWriteException;
import org.nur.protocol.RespValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class AofWriterTest {

    private Path aofFile;

    @BeforeEach
    void setUp() throws IOException {
        aofFile = Files.createTempFile("aof-test-", ".log");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(aofFile);
    }

    private RespValue.Array setCommand(String key, String value) {
        return new RespValue.Array(
                List.of(
                        new RespValue.BulkString("SET"),
                        new RespValue.BulkString(key),
                        new RespValue.BulkString(value)));
    }

    private String readAofFile() throws IOException {
        return Files.readString(aofFile);
    }

    @Test
    void appendedCommandIsWrittenToFile() throws IOException {
        try (AofWriter w = new AofWriter(aofFile)) {
            w.append(setCommand("foo", "bar"));
        }

        String content = readAofFile();
        assertTrue(content.contains("SET"), "Expected SET in AOF file");
        assertTrue(content.contains("foo"));
        assertTrue(content.contains("bar"));
    }

    @Test
    void multipleAppendsAreAllWrittenToFile() throws IOException {
        try (AofWriter w = new AofWriter(aofFile)) {
            w.append(setCommand("k1", "v1"));
            w.append(setCommand("k2", "v2"));
            w.append(setCommand("k3", "v3"));
        }

        String content = readAofFile();
        assertTrue(content.contains("k1"));
        assertTrue(content.contains("k2"));
        assertTrue(content.contains("k3"));
    }

    @Test
    void appendsAreWrittenInOrder() throws IOException {
        try (AofWriter w = new AofWriter(aofFile)) {
            w.append(setCommand("first", "1"));
            w.append(setCommand("second", "2"));
        }

        String content = readAofFile();
        int firstPos = content.indexOf("first");
        int secondPos = content.indexOf("second");
        assertTrue(firstPos < secondPos, "Expected 'first' to appear before 'second' in AOF");
    }

    @Test
    void appendToExistingFileDoesNotTruncate() throws IOException {
        try (AofWriter w = new AofWriter(aofFile)) {
            w.append(setCommand("existing", "value"));
        }

        try (AofWriter w = new AofWriter(aofFile)) {
            w.append(setCommand("new", "entry"));
        }

        String content = readAofFile();
        assertTrue(content.contains("existing"), "Original entry should still be present");
        assertTrue(content.contains("new"), "New entry should also be present");
    }

    @Test
    void closeWaitsForQueueToDrain() throws IOException {
        try (AofWriter w = new AofWriter(aofFile)) {
            for (int i = 0; i < 20; i++) {
                w.append(setCommand("key" + i, "val" + i));
            }
        }

        String content = readAofFile();
        for (int i = 0; i < 20; i++) {
            assertTrue(content.contains("key" + i), "Missing key" + i + " after close");
        }
    }

    @Test
    void closingEmptyWriterDoesNotThrow() {
        assertDoesNotThrow(
                () -> {
                    try (AofWriter w = new AofWriter(aofFile)) {
                        // nothing appended — close must still complete cleanly
                    }
                });
    }

    @Test
    void writtenContentIsValidResp() throws IOException {
        try (AofWriter w = new AofWriter(aofFile)) {
            w.append(setCommand("foo", "bar"));
        }

        try (var inputStream = Files.newInputStream(aofFile)) {
            var parser = new org.nur.protocol.RespParser(inputStream);
            RespValue parsed = parser.parse();

            assertInstanceOf(RespValue.Array.class, parsed);
            List<RespValue> elements = ((RespValue.Array) parsed).elements();
            assertEquals(new RespValue.BulkString("SET"), elements.get(0));
            assertEquals(new RespValue.BulkString("foo"), elements.get(1));
            assertEquals(new RespValue.BulkString("bar"), elements.get(2));
        }
    }

    @Test
    void aofQueueFullExceptionMessageContainsTimeoutMs() {
        AofQueueFullException ex = new AofQueueFullException(100);
        assertTrue(ex.getMessage().contains("100"), "Message should include timeout ms");
    }

    @Test
    void aofQueueFullExceptionDescribesCondition() {
        AofQueueFullException ex = new AofQueueFullException(100);
        String msg = ex.getMessage().toLowerCase();
        assertTrue(
                msg.contains("full") || msg.contains("queue"),
                "Message should describe the queue-full condition");
    }

    @Test
    void aofQueueFullExceptionIsRuntimeException() {
        assertInstanceOf(RuntimeException.class, new AofQueueFullException(100));
    }

    @Test
    void throwsWhenAofPathIsUnwritable() {
        Path badPath = Path.of("/nonexistent/dir/aof.log");
        // Construction throws before the thread or channel are created,
        // so there is nothing to close
        assertThrows(AofWriteException.class, () -> new AofWriter(badPath));
    }
}
