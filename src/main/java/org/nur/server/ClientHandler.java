package org.nur.server;

import org.nur.commands.CommandHandler;
import org.nur.exception.ClientDisconnectedException;
import org.nur.exception.RespProtocolException;
import org.nur.exception.ServerException;
import org.nur.protocol.RespParser;
import org.nur.protocol.RespWriter;
import org.nur.storage.StorageEngine;

import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private final Socket socket;
    private final CommandHandler handler;

    public ClientHandler(Socket socket, StorageEngine storageEngine) {
        this.socket = socket;
        this.handler = new CommandHandler(storageEngine);
    }

    @Override
    public void run() {
        String clientAddr = socket.getRemoteSocketAddress().toString();
        System.out.println("[" + clientAddr + "] Connected");

        try (socket;
                var outputStream = socket.getOutputStream()) {

            var parser = new RespParser(socket.getInputStream());
            var writer = new RespWriter(outputStream);

            while (true) {
                var command = parser.parse();
                System.out.println("[" + clientAddr + "] Received: " + command);
                var result = this.handler.handle(command);
                writer.write(result);
            }

        } catch (ClientDisconnectedException ignored) {
            System.out.println("[" + clientAddr + "] Disconnected");
        } catch (RespProtocolException e) {
            System.out.println("[" + clientAddr + "] Protocol error: " + e.getMessage());
        } catch (IOException e) {
            throw new ServerException("[" + clientAddr + "] Connection error", e);
        }
    }
}
