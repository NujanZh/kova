package org.nur.server;

import org.nur.commands.CommandHandler;
import org.nur.exception.ServerException;
import org.nur.persistence.AofLoader;
import org.nur.persistence.AofWriter;
import org.nur.storage.StorageEngine;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVServer {
    private static final int PORT = 6379;
    private static final Path AOF_FILE = Path.of("aof.log");

    public static void main(String[] args) {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        StorageEngine storageEngine = new StorageEngine();

        if (AOF_FILE.toFile().exists()) {
            try {
                AofLoader aofLoader = new AofLoader(new CommandHandler(storageEngine));
                aofLoader.load(AOF_FILE);
            } catch (IOException e) {
                throw new ServerException("[Server] Fatal error loading AOF", e);
            }
        }

        AofWriter aofWriter = new AofWriter(AOF_FILE);
        CommandHandler handler = new CommandHandler(storageEngine, aofWriter);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    System.out.println("[Server] Shutting down...");
                                    executor.shutdown();

                                    try {
                                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                                            executor.shutdownNow();
                                        }
                                    } catch (InterruptedException e) {
                                        executor.shutdownNow();
                                        Thread.currentThread().interrupt();
                                    }

                                    try {
                                        aofWriter.close();
                                    } catch (IOException e) {
                                        System.err.println(
                                                "[Server] Error closing AOF writer: "
                                                        + e.getMessage());
                                    }

                                    System.out.println("[Server] Shutdown complete");
                                }));

        try (var serverSocket = new ServerSocket(PORT)) {
            System.out.println("[Server] Started on port " + PORT);

            while (!executor.isShutdown()) {
                Socket clientSocket = serverSocket.accept();
                executor.execute(new ClientHandler(clientSocket, handler));
            }

        } catch (IOException e) {
            throw new ServerException("[Server] Fatal error", e);
        }
    }
}
