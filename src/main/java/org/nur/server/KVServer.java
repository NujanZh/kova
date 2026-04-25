package org.nur.server;

import org.nur.exception.ServerException;
import org.nur.protocol.RespParser;

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
                executor.execute(() -> handleClient(clientSocket));
            }

        } catch (IOException e) {
            throw new ServerException("[Server] Fatal error", e);
        }
    }

    private static void handleClient(Socket socket) {
        String clientAddr = socket.getRemoteSocketAddress().toString();
        System.out.println("[" + clientAddr + "] Connected");

        try (socket;
                var writer = new PrintWriter(socket.getOutputStream(), true)) {

            var parser = new RespParser(socket.getInputStream());
            var command = parser.parse();
            System.out.println("[" + clientAddr + "] Received: " + command);

            writer.println("OK");

        } catch (IOException e) {
            throw new ServerException("[" + clientAddr + "] Connection error", e);
        }

        System.out.println("[" + clientAddr + "] Disconnected");
    }
}
