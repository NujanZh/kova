package org.nur.persistence;

import org.nur.protocol.RespSerializer;
import org.nur.protocol.RespValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class AofWriter {

    private final FileChannel channel;

    public AofWriter(Path path) {
        try {
            this.channel =
                    FileChannel.open(
                            path,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND,
                            StandardOpenOption.DSYNC);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(RespValue command) throws IOException {
        byte[] bytes = RespSerializer.serialize(command).getBytes(StandardCharsets.UTF_8);
        int writtenBytes = channel.write(ByteBuffer.wrap(bytes));

        if (writtenBytes != bytes.length) {
            throw new IOException("Failed to write all bytes");
        }
    }
}
