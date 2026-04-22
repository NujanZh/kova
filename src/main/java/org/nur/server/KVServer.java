package org.nur.server;

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
            System.err.println("[Server] Fatal error: " + e.getMessage());
        }
    }

    private static void handleClient(Socket socket) {
        String clientAddr = socket.getRemoteSocketAddress().toString();
        System.out.println("[" + clientAddr + "] Connected");

        try (socket;
                var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                var writer = new PrintWriter(socket.getOutputStream(), true)) {

            String line;

            while ((line = reader.readLine()) != null) {
                System.out.println("[" + clientAddr + "] Received: " + line);
                writer.println("OK");
            }

        } catch (IOException e) {
            System.err.println("[" + clientAddr + "] Connection error: " + e.getMessage());
        }

        System.out.println("[" + clientAddr + "] Disconnected");
    }
}
