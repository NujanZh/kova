package org.nur.persistence;

import org.nur.protocol.RespSerializer;
import org.nur.protocol.RespValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class AofWriter implements AutoCloseable {

    private final BlockingDeque<byte[]> queue = new LinkedBlockingDeque<>();
    private final Thread worker;
    private volatile boolean running = true;

    private final FileChannel channel;

    public AofWriter(Path path) {
        try {
            this.channel =
                    FileChannel.open(
                            path,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND,
                            StandardOpenOption.DSYNC);

            worker = new Thread(this::processQueue, "AOF Writer");
            worker.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(RespValue command) {
        byte[] bytes = RespSerializer.serialize(command).getBytes(StandardCharsets.UTF_8);

        if (!queue.offer(bytes)) {
            throw new RuntimeException("Failed to append to AOF");
        }
    }

    @Override
    public void close() throws IOException {
        running = false;

        try {
            worker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        channel.close();
    }

    private void processQueue() {
        try {
            while (shouldContinue()) {
                byte[] bytes = takeBytes();
                if (bytes != null) {
                    writeBytes(bytes);
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean shouldContinue() {
        return running || !queue.isEmpty();
    }

    private byte[] takeBytes() throws InterruptedException {
        if (running) {
            queue.take();
        }

        return queue.poll();
    }

    private void writeBytes(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
