package org.nur.server;

import org.nur.exception.ClientDisconnectedException;
import org.nur.exception.RespProtocolException;
import org.nur.exception.ServerException;
import org.nur.protocol.RespParser;

import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private final Socket socket;

    public ClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        String clientAddr = socket.getRemoteSocketAddress().toString();
        System.out.println("[" + clientAddr + "] Connected");

        try (socket;
                var outputStream = socket.getOutputStream()) {

            var parser = new RespParser(socket.getInputStream());

            while (true) {
                var command = parser.parse();
                System.out.println("[" + clientAddr + "] Received: " + command);

                outputStream.write("+OK\r\n".getBytes());
                outputStream.flush();
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
