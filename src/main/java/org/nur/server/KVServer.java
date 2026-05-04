package org.nur.server;

import org.nur.commands.CommandHandler;
import org.nur.exception.ServerException;
import org.nur.persistence.AofLoader;
import org.nur.persistence.AofWriter;
import org.nur.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVServer {

    private static final Logger log = LoggerFactory.getLogger(KVServer.class);

    private static final int PORT = 6379;
    private static final Path AOF_FILE = Path.of("aof.log");

    public static void main(String[] args) {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        StorageEngine storageEngine = new StorageEngine();

        if (AOF_FILE.toFile().exists()) {
            log.info("Found AOF file at {}, replaying...", AOF_FILE);
            try {
                AofLoader aofLoader = new AofLoader(new CommandHandler(storageEngine));
                aofLoader.load(AOF_FILE);
                log.info("AOF replay completed");
            } catch (IOException e) {
                log.error("Fatal error loading AOF", e);
                throw new ServerException("[Server] Fatal error loading AOF", e);
            }
        } else {
            log.info("No AOF file found, starting fresh");
        }

        AofWriter aofWriter = new AofWriter(AOF_FILE);
        CommandHandler handler = new CommandHandler(storageEngine, aofWriter);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    log.info("Shutting down...");
                                    executor.shutdown();

                                    try {
                                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                                            log.warn(
                                                    "Executor did not terminate in 5 seconds, forcing shutdown");
                                            executor.shutdownNow();
                                        }
                                    } catch (InterruptedException e) {
                                        executor.shutdownNow();
                                        Thread.currentThread().interrupt();
                                    }

                                    try {
                                        aofWriter.close();
                                        log.info("AOF writer closed");
                                    } catch (IOException e) {
                                        log.error("Error closing AOF writer", e);
                                    }

                                    log.info("Shutdown complete");
                                }));

        try (var serverSocket = new ServerSocket(PORT)) {
            log.info("Started on port {}", PORT);

            while (!executor.isShutdown()) {
                Socket clientSocket = serverSocket.accept();
                log.debug("Accepted connection from {}", clientSocket.getRemoteSocketAddress());
                executor.execute(new ClientHandler(clientSocket, handler));
            }

        } catch (IOException e) {
            log.error("Fatal error in accept loop", e);
            throw new ServerException("Fatal error", e);
        }
    }
}
