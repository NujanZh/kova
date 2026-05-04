package org.nur.server;

import org.nur.commands.CommandHandler;
import org.nur.exception.ClientDisconnectedException;
import org.nur.exception.RespProtocolException;
import org.nur.exception.ServerException;
import org.nur.protocol.RespParser;
import org.nur.protocol.RespWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket socket;
    private final CommandHandler handler;

    public ClientHandler(Socket socket, CommandHandler handler) {
        this.socket = socket;
        this.handler = handler;
    }

    @Override
    public void run() {
        String clientAddr = socket.getRemoteSocketAddress().toString();
        log.info("Client connected: {}", clientAddr);

        try (socket;
                var outputStream = socket.getOutputStream()) {

            var parser = new RespParser(socket.getInputStream());
            var writer = new RespWriter(outputStream);

            while (true) {
                var command = parser.parse();
                log.debug("Received from {}: {}", clientAddr, command);
                var result = this.handler.handle(command);
                log.debug("Responding to {}: {}", clientAddr, result);
                writer.write(result);
            }

        } catch (ClientDisconnectedException ignored) {
            log.info("Client disconnected: {}", clientAddr);
        } catch (RespProtocolException e) {
            log.warn("Protocol error from {}: {}", clientAddr, e.getMessage());
        } catch (IOException e) {
            log.error("Connection error from {}", clientAddr, e);
            throw new ServerException("Connection error with " + clientAddr, e);
        }
    }
}
