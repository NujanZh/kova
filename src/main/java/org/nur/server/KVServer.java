package org.nur.server;

import org.nur.exception.ServerException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVServer {
    private static final int PORT = 6379;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    System.out.println("[Server] Shutting down...");
                                    executor.shutdown();
                                }));

        try (var serverSocket = new ServerSocket(PORT)) {
            System.out.println("[Server] Started on port " + PORT);

            while (!executor.isShutdown()) {
                Socket clientSocket = serverSocket.accept();
                executor.execute(new ClientHandler(clientSocket));
            }

        } catch (IOException e) {
            throw new ServerException("[Server] Fatal error", e);
        }
    }
}
