package org.nur.persistence;

import org.nur.exception.AofQueueFullException;
import org.nur.exception.AofWriteException;
import org.nur.protocol.RespSerializer;
import org.nur.protocol.RespValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class AofWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AofWriter.class);

    private static final int QUEUE_CAPACITY = 4096;
    private static final int OFFER_TIMEOUT_MS = 100;

    private final BlockingDeque<byte[]> queue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);
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

            log.debug("AOF file opened: {}", path);

            worker = new Thread(this::processQueue, "AOF Writer");
            worker.start();

            log.debug("AOF writer background thread started");
        } catch (IOException e) {
            log.error("Failed to open AOF file at {}", path, e);
            throw new AofWriteException("Failed to open AOF file", e);
        }
    }

    public void append(RespValue command) {
        byte[] bytes = RespSerializer.serialize(command).getBytes(StandardCharsets.UTF_8);

        try {
            boolean accepted = queue.offer(bytes, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!accepted) {
                log.error(
                        "AOF queue is full after {}ms, current queue size: {}",
                        OFFER_TIMEOUT_MS,
                        queue.size());
                throw new AofQueueFullException(OFFER_TIMEOUT_MS);
            }

            log.debug(
                    "Enqueued {} bytes, queue size: {}/{}",
                    bytes.length,
                    queue.size(),
                    QUEUE_CAPACITY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting to append to AOF", e);
            throw new AofWriteException("Interrupted while waiting to append to AOF", e);
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Closing AOF writer, draining {} remaining item(s)...", queue.size());
        running = false;

        try {
            worker.join();
            log.info("AOF writer thread drained and stopped");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for AOF writer to drain", e);
            throw new AofWriteException("Interrupted while waiting for AOF writer to drain", e);
        }

        channel.close();
        log.info("AOF file channel closed");
    }

    private void processQueue() {
        log.debug("AOF writer thread running");
        try {
            while (running || !queue.isEmpty()) {
                byte[] bytes = queue.poll(OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (bytes != null) {
                    writeBytes(bytes);
                    log.debug("Wrote {} bytes to AOF", bytes.length);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("AOF writer thread interrupted", e);
            throw new AofWriteException("AOF writer interrupted", e);
        } catch (IOException e) {
            log.error("AOF writer failed to write to disk", e);
            throw new AofWriteException("AOF writer I/O error", e);
        }
    }

    private void writeBytes(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
